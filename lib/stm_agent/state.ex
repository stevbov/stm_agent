defmodule StmAgent.State do
  @moduledoc false
  defstruct data: nil,
            version: 1,
            tx_data: %{},
            tx_version: %{},
            tx_verifying: nil,
            tx_on_commit: %{},
            tx_on_abort: %{},
            retry_queue: []

  def new(data) do
    %StmAgent.State{
      data: data
    }
  end

  def get(state, tx, fun) do
    case access(state, tx) do
      {:ok, state} ->
        try do
          data = Map.get(state.tx_data, tx, state.data)
          reply = fun.(data)
          {:ok, reply, state}
        rescue
          _ in StmAgent.AbortError ->
            {:abort, state}
        end

      access_result ->
        access_result
    end
  end

  def update(state, tx, fun) do
    case access(state, tx) do
      {:ok, state} ->
        try do
          data = Map.get(state.tx_data, tx, state.data)
          data = fun.(data)
          {:ok, %{state | tx_data: Map.put(state.tx_data, tx, data)}}
        rescue
          _ in StmAgent.AbortError ->
            {:abort, state}
        end

      access_result ->
        access_result
    end
  end

  def get_and_update(state, tx, fun) do
    case access(state, tx) do
      {:ok, state} ->
        try do
          data = Map.get(state.tx_data, tx, state.data)
          {reply, data} = fun.(data)
          {:ok, reply, %{state | tx_data: Map.put(state.tx_data, tx, data)}}
        rescue
          _ in StmAgent.AbortError ->
            {:abort, state}
        end

      access_result ->
        access_result
    end
  end

  def verify(%{tx_verifying: nil} = state, tx) do
    tx_version = Map.get(state.tx_version, tx, state.version)

    if tx_version == state.version do
      {:ok, %{state | tx_verifying: tx}}
    else
      :abort
    end
  end

  def verify(%{tx_verifying: tx} = state, tx) do
    {:ok, state}
  end

  def verify(_state, _tx) do
    :abort
  end

  def commit(%{tx_verifying: tx} = state, tx) do
    new_version = if Map.has_key?(state.tx_data, tx), do: state.version + 1, else: state.version
    new_data = Map.get(state.tx_data, tx, state.data)

    Map.get(state.tx_on_commit, tx, [])
    |> Enum.reverse()
    |> Enum.each(fn fun -> fun.(new_data) end)

    {:ok,
     %{
       state
       | data: new_data,
         version: new_version,
         tx_data: Map.delete(state.tx_data, tx),
         tx_version: Map.delete(state.tx_version, tx),
         tx_on_abort: Map.delete(state.tx_on_abort, tx),
         tx_on_commit: Map.delete(state.tx_on_commit, tx),
         tx_verifying: nil
     }}
  end

  def commit(_state, _tx) do
    :error
  end

  def abort(state, tx) do
    new_tx_verifying = if state.tx_verifying == tx, do: nil, else: state.tx_verifying

    Map.get(state.tx_on_abort, tx, [])
    |> Enum.reverse()
    |> Enum.each(fn fun -> fun.(state.data) end)

    %{
      state
      | tx_data: Map.delete(state.tx_data, tx),
        tx_version: Map.delete(state.tx_version, tx),
        tx_on_abort: Map.delete(state.tx_on_abort, tx),
        tx_on_commit: Map.delete(state.tx_on_commit, tx),
        tx_verifying: new_tx_verifying
    }
  end

  def on_commit(state, tx, fun) do
    on_commit = Map.get(state.tx_on_commit, tx, [])
    %{state | tx_on_commit: Map.put(state.tx_on_commit, tx, [fun | on_commit])}
  end

  def on_abort(state, tx, fun) do
    on_abort = Map.get(state.tx_on_abort, tx, [])
    %{state | tx_on_abort: Map.put(state.tx_on_abort, tx, [fun | on_abort])}
  end

  def dirty_get(state, fun) do
    fun.(state.data)
  end

  def dirty_update(state, fun, from) do
    if state.tx_verifying != nil do
      new_retry_queue = [from | state.retry_queue]
      {:retry, %{state | retry_queue: new_retry_queue}}
    else
      {:ok, %{state | data: fun.(state.data), version: state.version + 1}}
    end
  end

  def dirty_get_and_update(state, fun, from) do
    if state.tx_verifying != nil do
      new_retry_queue = [from | state.retry_queue]
      {:retry, %{state | retry_queue: new_retry_queue}}
    else
      {reply, new_data} = fun.(state.data)
      {:ok, reply, %{state | data: new_data, version: state.version + 1}}
    end
  end

  def clear_retry_queue(state) do
    {%{state | retry_queue: []}, Enum.reverse(state.retry_queue)}
  end

  defp access(state, tx) do
    new_state = %{state | tx_version: Map.put_new(state.tx_version, tx, state.version)}
    tx_version = Map.get(new_state.tx_version, tx)

    cond do
      # If we're verifying another transaction that has modified data then we are likely going to need to abort if we
      # allow access, so let's just :abort now.
      state.tx_verifying != nil && state.tx_verifying != tx &&
          Map.has_key?(state.tx_data, state.tx_verifying) ->
        {:abort, new_state}

      tx_version == state.version ->
        {:ok, new_state}

      true ->
        {:abort, new_state}
    end
  end
end

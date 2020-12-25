defmodule StmAgent.Server do
  @moduledoc false
  use GenServer

  def init(fun) do
    {:ok, StmAgent.State.new(fun.())}
  end

  def handle_call({:get, fun, tx}, _from, state) do
    case StmAgent.State.get(state, fun, tx) do
      {:ok, reply, state} ->
        {:reply, {:ok, reply}, state}

      {:abort, new_state} ->
        {:reply, :abort, new_state}
    end
  end

  def handle_call({:update, fun, tx}, _from, state) do
    case StmAgent.State.update(state, fun, tx) do
      {:ok, state} ->
        {:reply, :ok, state}

      {:abort, new_state} ->
        {:reply, :abort, new_state}
    end
  end

  def handle_call({:get_and_update, fun, tx}, _from, state) do
    case StmAgent.State.get_and_update(state, fun, tx) do
      {:ok, reply, state} ->
        {:reply, {:ok, reply}, state}

      {:abort, new_state} ->
        {:reply, :abort, new_state}
    end
  end

  def handle_call({:verify, tx}, _from, state) do
    case StmAgent.State.verify(state, tx) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      :abort ->
        {:reply, :abort, state}
    end
  end

  def handle_call({:commit, tx}, _from, state) do
    case StmAgent.State.commit(state, tx) do
      {:ok, new_state} ->
        Process.send(self(), :process_retry_queue, [])
        {:reply, :ok, new_state}

      :error ->
        {:reply, :error, state}
    end
  end

  def handle_call({:abort, tx}, _from, state) do
    new_state = StmAgent.State.abort(state, tx)
    Process.send(self(), :process_retry_queue, [])
    {:reply, :ok, new_state}
  end

  def handle_call({:dirty_get, fun}, _from, state) do
    reply = StmAgent.State.dirty_get(state, fun)
    {:reply, reply, state}
  end

  def handle_call({:dirty_update, fun}, from, state) do
    case StmAgent.State.dirty_update(state, fun, from) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:retry, new_state} ->
        {:noreply, new_state}
    end
  end

  def handle_call({:dirty_get_and_update, fun}, from, state) do
    case StmAgent.State.dirty_get_and_update(state, fun, from) do
      {:ok, reply, new_state} ->
        {:reply, reply, new_state}

      {:retry, new_state} ->
        {:noreply, new_state}
    end
  end

  def handle_cast({:cast, fun, tx}, state) do
    case StmAgent.State.update(state, fun, tx) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:abort, new_state} ->
        {:noreply, new_state}
    end
  end

  def handle_info(:process_retry_queue, state) do
    {new_state, retry_queue} = StmAgent.State.clear_retry_queue(state)
    Enum.each(retry_queue, fn from -> GenServer.reply(from, :retry) end)
    {:noreply, new_state}
  end
end


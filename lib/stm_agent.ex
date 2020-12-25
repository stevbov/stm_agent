defmodule StmAgent do
  @moduledoc """
  Similar to Agent, but access and modification of state is controlled with Software Transactional Memory (STM).

  Most commonly used inside the StmAgent.Transaction.transaction function.
  """

  def start_link(fun, options \\ []) do
    GenServer.start_link(StmAgent.Server, fun, options)
  end

  def start(fun, options \\ []) do
    GenServer.start(StmAgent.Server, fun, options)
  end

  def stop(pid, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(pid, reason, timeout)
  end

  def get(pid, fun, tx) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.call(pid, {:get, fun, tx})
  end

  def update(pid, fun, tx) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.call(pid, {:update, fun, tx})
  end

  def get_and_update(pid, fun, tx) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.call(pid, {:get_and_update, fun, tx})
  end

  def cast(pid, fun, tx) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.cast(pid, {:cast, fun, tx})
  end

  def get!(pid, fun, tx) do
    case get(pid, fun, tx) do
      {:ok, value} ->
        value

      :abort ->
        raise StmAgent.AbortError
    end
  end

  def update!(pid, fun, tx) do
    case update(pid, fun, tx) do
      :ok ->
        :ok

      :abort ->
        raise StmAgent.AbortError
    end
  end

  def get_and_update!(pid, fun, tx) do
    case get_and_update(pid, fun, tx) do
      {:ok, value} ->
        value

      :abort ->
        raise StmAgent.AbortError
    end
  end

  def verify(pid, tx) do
    if :global.whereis_name(tx) != self() do
      StmAgent.TransactionMonitor.accessed(tx, pid)
    end

    GenServer.call(pid, {:verify, tx})
  end

  def commit(pid, tx) do
    GenServer.call(pid, {:commit, tx})
  end

  def abort(pid, tx) do
    GenServer.call(pid, {:abort, tx})
  end

  def dirty_get(pid, fun) do
    GenServer.call(pid, {:dirty_get, fun})
  end

  def dirty_update(pid, fun) do
    case GenServer.call(pid, {:dirty_update, fun}) do
      :retry ->
        dirty_update(pid, fun)

      result ->
        result
    end
  end

  def dirty_get_and_update(pid, fun) do
    case GenServer.call(pid, {:dirty_get_and_update, fun}) do
      :retry ->
        dirty_get_and_update(pid, fun)

      result ->
        result
    end
  end
end

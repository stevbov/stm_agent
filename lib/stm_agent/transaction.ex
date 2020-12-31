defmodule StmAgent.Transaction do
  @moduledoc """
  Used for transactions with StmAgent.
  """

  def transaction(fun, retries \\ :infinity) do
    tx = StmAgent.Transaction.Id.new()
    StmAgent.TransactionMonitor.start_link(tx)

    try do
      transaction(tx, fun, retries)
    rescue
      _ in StmAgent.AbortError ->
        StmAgent.TransactionMonitor.abort(tx)
    after
      StmAgent.TransactionMonitor.stop(tx)
    end
  end

  @doc """
  Called after transaction is verified.

  Use this if something needs to run atomically on successful commit.  If you use on_commit there can be a race
  condition - on_commit callbacks run after StmAgents are committed (and the verify lock is gone) which means another
  process could modify it before on_commit callbacks are called.
  """
  def on_verify(tx, fun) do
    StmAgent.TransactionMonitor.on_verify(tx, fun)
  end

  @doc """
  Called after transaction is committed.

  If you need this to happen atomically with the transaction, use on_verify instead.
  """
  def on_commit(tx, fun) do
    StmAgent.TransactionMonitor.on_commit(tx, fun)
  end

  @doc "Called after transaction is aborted"
  def on_abort(tx, fun) do
    StmAgent.TransactionMonitor.on_abort(tx, fun)
  end

  defp transaction(tx, fun, retries) do
    result = fun.(tx)

    case StmAgent.TransactionMonitor.commit(tx) do
      :aborted when retries == :infinity ->
        transaction(tx, fun, retries)

      :aborted when retries > 1 ->
        transaction(tx, fun, retries - 1)

      :aborted ->
        :aborted

      :ok ->
        {:ok, result}
    end
  end

  defmodule Id do
    @moduledoc false
    defstruct id: nil

    def new() do
      %StmAgent.Transaction.Id{id: UUID.uuid4()}
    end
  end
end

defmodule StmAgent.TransactionMonitor do
  # Keeps track of which StmAgents a transaction has accessed and handles committing and rolling back of those
  # StmAgents.
  @moduledoc false

  defstruct tx: nil, accessed_pids: [], on_verify: [], on_commit: [], on_abort: []

  alias StmAgent.TransactionMonitor

  def start_link(tx) do
    GenServer.start_link(__MODULE__, tx, name: {:global, tx})
  end

  def stop(tx) do
    GenServer.stop({:global, tx})
  end

  def accessed(tx, pid) do
    GenServer.cast({:global, tx}, {:accessed, pid})
  end

  def abort(tx) do
    GenServer.call({:global, tx}, :abort)
  end

  def commit(tx) do
    GenServer.call({:global, tx}, :commit)
  end

  def on_verify(tx, fun) do
    GenServer.call({:global, tx}, {:on_verify, fun})
  end

  def on_commit(tx, fun) do
    GenServer.call({:global, tx}, {:on_commit, fun})
  end

  def on_abort(tx, fun) do
    GenServer.call({:global, tx}, {:on_abort, fun})
  end

  # GenServer callbacks
  def init(tx) do
    {:ok, %TransactionMonitor{tx: tx}}
  end

  def handle_call({:on_verify, fun}, _from, %{on_verify: on_verify} = state) do
    new_on_verify = [fun | on_verify]
    {:reply, :ok, %{state | on_verify: new_on_verify}}
  end

  def handle_call({:on_abort, fun}, _from, %{on_abort: on_abort} = state) do
    new_on_abort = [fun | on_abort]
    {:reply, :ok, %{state | on_abort: new_on_abort}}
  end

  def handle_call({:on_commit, fun}, _from, %{on_commit: on_commit} = state) do
    new_on_commit = [fun | on_commit]
    {:reply, :ok, %{state | on_commit: new_on_commit}}
  end

  def handle_call(:abort, _from, %{tx: tx, accessed_pids: accessed_pids, on_abort: on_abort}) do
    Enum.each(accessed_pids, fn pid -> :ok = StmAgent.abort(pid, tx) end)
    Enum.each(on_abort, fn fun -> fun.() end)

    {:reply, :ok, %TransactionMonitor{tx: tx}}
  end

  def handle_call(:commit, _from, state) do
    verify_results =
      Enum.map(state.accessed_pids, fn pid ->
        try do
          {StmAgent.verify(pid, state.tx), pid}
        catch
          :exit, _ -> {:error, pid}
        end
      end)

    verify_ok = Enum.all?(verify_results, fn {result, _pid} -> result == :ok end)

    if verify_ok do
      Enum.each(state.on_verify, fn fun -> fun.() end)

      verify_results
      |> Enum.each(fn {_result, pid} -> :ok = StmAgent.commit(pid, state.tx) end)

      Enum.each(state.on_commit, fn fun -> fun.() end)

      {:reply, :ok, %TransactionMonitor{tx: state.tx}}
    else
      verify_results
      |> Enum.filter(fn {result, _pid} -> result != :error end)
      |> Enum.each(fn {_result, pid} -> :ok = StmAgent.abort(pid, state.tx) end)

      Enum.each(state.on_abort, fn fun -> fun.() end)

      {:reply, :aborted, %TransactionMonitor{tx: state.tx}}
    end
  end

  def handle_cast({:accessed, pid}, %{accessed_pids: accessed_pids} = state) do
    new_accessed_pids =
      if Enum.member?(accessed_pids, pid) do
        accessed_pids
      else
        [pid | accessed_pids]
      end

    {:noreply, %{state | accessed_pids: new_accessed_pids}}
  end

  def terminate(_reason, %{tx: tx, accessed_pids: accessed_pids, on_abort: on_abort}) do
    accessed_pids
    |> Enum.each(fn pid -> :ok = StmAgent.abort(pid, tx) end)

    Enum.each(on_abort, fn fun -> fun.() end)

    :ok
  end
end

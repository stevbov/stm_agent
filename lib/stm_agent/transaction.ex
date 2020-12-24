defmodule StmAgent.Transaction do
  defmodule Id do
    defstruct id: nil

    def new() do
      %StmAgent.Transaction.Id{id: UUID.uuid4()}
    end
  end

  def transaction(fun, retries \\ :infinity) do
    tx = StmAgent.Transaction.Id.new()
    StmAgent.TransactionMonitor.start_link(tx)

    try do
      transaction(fun, retries, tx)
    rescue
      _ in StmAgent.AbortError ->
        StmAgent.TransactionMonitor.abort(tx)
    after
      StmAgent.TransactionMonitor.stop(tx)
    end
  end

  defp transaction(fun, retries, tx) do
    result = fun.(tx)

    case StmAgent.TransactionMonitor.commit(tx) do
      :aborted when retries == :infinity ->
        transaction(fun, retries, tx)

      :aborted when retries > 1 ->
        transaction(fun, retries - 1, tx)

      :aborted ->
        :aborted

      :ok ->
        {:ok, result}
    end
  end
end

defmodule StmAgent.TransactionMonitor do
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

  # GenServer callbacks
  def init(tx) do
    {:ok, {tx, []}}
  end

  def handle_call(:abort, _from, {tx, accessed_pids}) do
    accessed_pids
    |> Enum.each(fn pid -> :ok = StmAgent.abort(pid, tx) end)

    {:reply, :ok, {tx, []}}
  end

  def handle_call(:commit, _from, {tx, accessed_pids}) do
    verify_results =
      Enum.map(accessed_pids, fn pid ->
        try do
          {StmAgent.verify(pid, tx), pid}
        catch
          :exit, _ -> {:error, pid}
        end
      end)

    verify_ok = Enum.all?(verify_results, fn {result, _pid} -> result == :ok end)

    if verify_ok do
      verify_results
      |> Enum.each(fn {_result, pid} -> :ok = StmAgent.commit(pid, tx) end)

      {:reply, :ok, {tx, []}}
    else
      verify_results
      |> Enum.filter(fn {result, _pid} -> result != :error end)
      |> Enum.each(fn {_result, pid} -> :ok = StmAgent.abort(pid, tx) end)

      {:reply, :aborted, {tx, []}}
    end
  end

  def handle_cast({:accessed, pid}, {tx, accessed_pids}) do
    new_accessed_pids =
      if Enum.member?(accessed_pids, pid) do
        accessed_pids
      else
        [pid | accessed_pids]
      end

    {:noreply, {tx, new_accessed_pids}}
  end

  def terminate(_reason, {tx, accessed_pids}) do
    accessed_pids
    |> Enum.each(fn pid -> :ok = StmAgent.abort(pid, tx) end)

    :ok
  end
end


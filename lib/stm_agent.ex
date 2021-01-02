defmodule StmAgent do
  @moduledoc """
  Similar to Agent, but access and modification of state is controlled with Software Transactional Memory (STM).

  Most commonly used inside the StmAgent.Transaction.transaction function.
  """

  def child_spec(arg) do
    %{
      id: StmAgent,
      start: {StmAgent, :start_link, [arg]}
    }
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      unless Module.has_attribute?(__MODULE__, :doc) do
        @doc """
        Returns a specification to start this module under a supervisor.
        See `Supervisor`.
        """
      end

      def child_spec(arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  def start_link(fun, options \\ []) do
    GenServer.start_link(StmAgent.Server, fun, options)
  end

  def start(fun, options \\ []) do
    GenServer.start(StmAgent.Server, fun, options)
  end

  def stop(pid, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(pid, reason, timeout)
  end

  def get(pid, tx, fun) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.call(pid, {:get, tx, fun})
  end

  def update(pid, tx, fun) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.call(pid, {:update, tx, fun})
  end

  def get_and_update(pid, tx, fun) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.call(pid, {:get_and_update, tx, fun})
  end

  def cast(pid, tx, fun) do
    StmAgent.TransactionMonitor.accessed(tx, pid)
    GenServer.cast(pid, {:cast, tx, fun})
  end

  def get!(pid, tx, fun) do
    case get(pid, tx, fun) do
      {:ok, value} ->
        value

      :abort ->
        raise StmAgent.AbortError
    end
  end

  def update!(pid, tx, fun) do
    case update(pid, tx, fun) do
      :ok ->
        :ok

      :abort ->
        raise StmAgent.AbortError
    end
  end

  def get_and_update!(pid, tx, fun) do
    case get_and_update(pid, tx, fun) do
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

  def on_commit(pid, tx, fun) do
    GenServer.call(pid, {:on_commit, tx, fun})
  end

  def on_abort(pid, tx, fun) do
    GenServer.call(pid, {:on_abort, tx, fun})
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

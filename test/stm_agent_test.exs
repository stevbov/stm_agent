defmodule StmAgentTest do
  use ExUnit.Case

  setup do
    {:ok, agent} = StmAgent.start(fn -> 1 end)

    on_exit(fn ->
      if Process.alive?(agent) do
        StmAgent.stop(agent)
      end
    end)

    tx = StmAgent.Transaction.Id.new()
    tx2 = StmAgent.Transaction.Id.new()

    [agent: agent, tx: tx, tx2: tx2]
  end

  describe "get" do
    test "basic", context do
      assert {:ok, 2} = StmAgent.get(context.agent, context.tx, fn v -> v + 1 end)
    end
  end

  describe "update" do
    test "basic", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)

      assert {:ok, 3} = StmAgent.get(context.agent, context.tx, fn v -> v end)
    end
  end

  describe "cast" do
    test "basic", context do
      :ok = StmAgent.cast(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.cast(context.agent, context.tx, fn v -> v + 1 end)

      assert {:ok, 3} = StmAgent.get(context.agent, context.tx, fn v -> v end)
    end

    test "aborted", context do
      :ok = StmAgent.update(context.agent, context.tx2, fn v -> v + 1 end)
      :ok = StmAgent.verify(context.agent, context.tx2)

      :ok = StmAgent.cast(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.abort(context.agent, context.tx2)

      assert {:ok, 1} = StmAgent.get(context.agent, context.tx, fn v -> v end)
    end
  end

  describe "get_and_update" do
    test "basic", context do
      {:ok, 3} = StmAgent.get_and_update(context.agent, context.tx, fn v -> {v + 2, v + 1} end)
      {:ok, 4} = StmAgent.get_and_update(context.agent, context.tx, fn v -> {v + 2, v + 1} end)

      assert {:ok, 3} = StmAgent.get(context.agent, context.tx, fn v -> v end)
    end
  end

  describe "verify" do
    test "basic", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)

      assert :ok = StmAgent.verify(context.agent, context.tx)
    end
  end

  describe "commit" do
    test "basic", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.verify(context.agent, context.tx)

      assert :ok = StmAgent.commit(context.agent, context.tx)
      assert {:ok, 2} = StmAgent.get(context.agent, context.tx2, fn v -> v end)
    end
  end

  describe "abort" do
    test "basic", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.verify(context.agent, context.tx)

      assert :ok = StmAgent.abort(context.agent, context.tx)
      assert {:ok, 1} = StmAgent.get(context.agent, context.tx2, fn v -> v end)
    end
  end

  describe "dirty get" do
    test "basic", context do
      assert 2 == StmAgent.dirty_get(context.agent, fn v -> v + 1 end)
    end
  end

  describe "dirty update" do
    test "basic", context do
      assert :ok = StmAgent.dirty_update(context.agent, fn v -> v + 3 end)
      assert 4 = StmAgent.dirty_get(context.agent, fn v -> v end)
    end

    test "after verifying transaction commits, retries dirty_update", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.verify(context.agent, context.tx)

      task =
        Task.async(fn ->
          StmAgent.dirty_update(context.agent, fn v -> v + 3 end)
        end)

      # TODO - need to figure out a reliable way of making sure StmAgent.dirty_update is called before we commit
      Process.sleep(1)
      :ok = StmAgent.commit(context.agent, context.tx)

      assert :ok = Task.await(task)
      assert 5 = StmAgent.dirty_get(context.agent, fn v -> v end)
    end

    test "after verifying transaction aborts, retries dirty_update", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.verify(context.agent, context.tx)

      task =
        Task.async(fn ->
          StmAgent.dirty_update(context.agent, fn v -> v + 3 end)
        end)

      # TODO - need to figure out a reliable way of making sure StmAgent.dirty_update is called before we commit
      Process.sleep(1)
      :ok = StmAgent.abort(context.agent, context.tx)

      assert :ok = Task.await(task)
      assert 4 = StmAgent.dirty_get(context.agent, fn v -> v end)
    end

    test "multiple retries", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.verify(context.agent, context.tx)

      task =
        Task.async(fn ->
          StmAgent.dirty_update(context.agent, fn v -> v + 3 end)
        end)

      task2 =
        Task.async(fn ->
          StmAgent.dirty_update(context.agent, fn v -> v + 5 end)
        end)

      # TODO - need to figure out a reliable way of making sure StmAgent.dirty_update is called before we commit
      Process.sleep(1)
      :ok = StmAgent.commit(context.agent, context.tx)

      assert :ok = Task.await(task)
      assert :ok = Task.await(task2)
      assert 10 = StmAgent.dirty_get(context.agent, fn v -> v end)
    end
  end

  describe "dirty get_and_update" do
    test "basic", context do
      assert 5 = StmAgent.dirty_get_and_update(context.agent, fn v -> {v + 4, v + 3} end)
      assert 4 = StmAgent.dirty_get(context.agent, fn v -> v end)
    end

    test "multiple retries", context do
      :ok = StmAgent.update(context.agent, context.tx, fn v -> v + 1 end)
      :ok = StmAgent.verify(context.agent, context.tx)

      task =
        Task.async(fn ->
          StmAgent.dirty_get_and_update(context.agent, fn v -> {3, v + 3} end)
        end)

      task2 =
        Task.async(fn ->
          StmAgent.dirty_get_and_update(context.agent, fn v -> {4, v + 5} end)
        end)

      # TODO - need to figure out a reliable way of making sure StmAgent.dirty_update is called before we commit
      Process.sleep(1)
      :ok = StmAgent.commit(context.agent, context.tx)

      assert 3 = Task.await(task)
      assert 4 = Task.await(task2)
      assert 10 = StmAgent.dirty_get(context.agent, fn v -> v end)
    end
  end

  describe "callbacks" do
    test "on_abort properly set", context do
      :ok = StmAgent.on_abort(context.agent, context.tx, fn _v -> :ok end)
      state = :sys.get_state(context.agent)
      assert 1 == Enum.count(Map.get(state.tx_on_abort, context.tx))
    end

    test "on_commit properly set", context do
      :ok = StmAgent.on_commit(context.agent, context.tx, fn _v -> :ok end)
      state = :sys.get_state(context.agent)
      assert 1 == Enum.count(Map.get(state.tx_on_commit, context.tx))
    end
  end
end

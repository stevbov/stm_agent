defmodule StmAgent.TransactionTest do
  use ExUnit.Case

  setup do
    {:ok, agent} = StmAgent.start(fn -> 1 end)

    on_exit(fn ->
      if Process.alive?(agent) do
        StmAgent.stop(agent)
      end
    end)

    {:ok, agent2} = StmAgent.start(fn -> 100 end)

    on_exit(fn ->
      if Process.alive?(agent2) do
        StmAgent.stop(agent2)
      end
    end)

    [agent: agent, agent2: agent2]
  end

  test "basic", context do
    {:ok, _} =
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
      end)

    assert 2 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "with reply", context do
    {:ok, 13} =
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
        13
      end)

    assert 2 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "multiple updates", context do
    {:ok, _} =
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
        StmAgent.update(context.agent2, fn v -> v + 5 end, tx)
        StmAgent.update(context.agent, fn v -> v + 3 end, tx)
        StmAgent.update(context.agent2, fn v -> v + 7 end, tx)
      end)

    assert 5 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)

    assert 112 = StmAgent.dirty_get(context.agent2, fn v -> v end)
    state = :sys.get_state(context.agent2)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "manual abort", context do
    {:ok, _} =
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
        StmAgent.abort(context.agent, tx)
      end)

    assert 1 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "raise cleans out transaction info", context do
    try do
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
        raise "abort"
      end)
    rescue
      _ -> :ok
    end

    assert 1 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "throw cleans out transaction info", context do
    try do
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
        throw("abort")
      end)
    catch
      _ -> :ok
    end

    assert 1 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "exit cleans out transaction info", context do
    try do
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
        exit("abort")
      end)
    catch
      :exit, _ -> :ok
    end

    assert 1 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "multiple agents aborted", context do
    try do
      StmAgent.Transaction.transaction(fn tx ->
        StmAgent.update(context.agent, fn v -> v + 1 end, tx)
        StmAgent.update(context.agent2, fn v -> v + 7 end, tx)
        raise "abort"
      end)
    rescue
      _ -> :ok
    end

    assert 1 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)

    assert 100 = StmAgent.dirty_get(context.agent2, fn v -> v end)
    state = :sys.get_state(context.agent2)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "failed update, causes abort", context do
    tx2 = StmAgent.Transaction.Id.new()
    :ok = StmAgent.update(context.agent, fn v -> v + 1 end, tx2)
    :ok = StmAgent.verify(context.agent, tx2)

    assert :aborted =
             StmAgent.Transaction.transaction(
               fn tx ->
                 :abort = StmAgent.update(context.agent, fn v -> v + 1 end, tx)
               end,
               1
             )

    assert 1 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert 1 == Enum.count(state.tx_data)
    assert 1 == Enum.count(state.tx_version)
  end

  test "failed update, outer tx commits, we exit transaction(): aborts", context do
    tx2 = StmAgent.Transaction.Id.new()
    :ok = StmAgent.update(context.agent, fn v -> v + 1 end, tx2)
    :ok = StmAgent.verify(context.agent, tx2)

    assert :aborted =
             StmAgent.Transaction.transaction(
               fn tx ->
                 :abort = StmAgent.update(context.agent, fn v -> v + 5 end, tx)
                 :ok = StmAgent.commit(context.agent, tx2)
               end,
               1
             )

    assert 2 = StmAgent.dirty_get(context.agent, fn v -> v end)
    state = :sys.get_state(context.agent)
    assert Enum.empty?(state.tx_data)
    assert Enum.empty?(state.tx_version)
  end

  @tag capture_log: true
  test "stopped agent causes abort", context do
    assert :aborted =
             StmAgent.Transaction.transaction(
               fn tx ->
                 StmAgent.update(context.agent, fn v -> v + 1 end, tx)
                 StmAgent.update(context.agent2, fn v -> v + 7 end, tx)
                 StmAgent.stop(context.agent)
               end,
               1
             )

    assert 100 = StmAgent.dirty_get(context.agent2, fn v -> v end)
    state = :sys.get_state(context.agent2)
    assert nil == state.tx_verifying
    assert Enum.empty?(state.tx_version)
    assert Enum.empty?(state.tx_data)
  end

  test "dirty update in transaction causes it to always fail", context do
    assert :aborted =
             StmAgent.Transaction.transaction(
               fn tx ->
                 :ok = StmAgent.update(context.agent, fn v -> v + 5 end, tx)
                 StmAgent.dirty_update(context.agent, fn v -> v + 3 end)
               end,
               5
             )

    assert 16 = StmAgent.dirty_get(context.agent, fn v -> v end)
  end

  describe "callbacks" do
    test "when committing", context do
      {:ok, verify_counter} = Agent.start_link(fn -> 0 end)
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      {:ok, _} =
        StmAgent.Transaction.transaction(fn tx ->
          StmAgent.Transaction.on_verify(tx, fn ->
            Agent.update(verify_counter, fn v -> v + 1 end)
          end)

          StmAgent.Transaction.on_commit(tx, fn ->
            Agent.update(commit_counter, fn v -> v + 1 end)
          end)

          StmAgent.Transaction.on_abort(tx, fn ->
            Agent.update(abort_counter, fn v -> v + 1 end)
          end)

          :ok = StmAgent.update(context.agent, fn v -> v + 5 end, tx)
        end)

      assert 1 = Agent.get(commit_counter, fn v -> v end)
      assert 1 = Agent.get(verify_counter, fn v -> v end)
      assert 0 = Agent.get(abort_counter, fn v -> v end)
    end

    test "when aborting", context do
      {:ok, verify_counter} = Agent.start_link(fn -> 0 end)
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      tx2 = StmAgent.Transaction.Id.new()
      :ok = StmAgent.update(context.agent, fn v -> v + 1 end, tx2)
      :ok = StmAgent.verify(context.agent, tx2)

      :aborted =
        StmAgent.Transaction.transaction(
          fn tx ->
            StmAgent.Transaction.on_verify(tx, fn ->
              Agent.update(verify_counter, fn v -> v + 1 end)
            end)

            StmAgent.Transaction.on_commit(tx, fn ->
              Agent.update(commit_counter, fn v -> v + 1 end)
            end)

            StmAgent.Transaction.on_abort(tx, fn ->
              Agent.update(abort_counter, fn v -> v + 1 end)
            end)

            :abort = StmAgent.update(context.agent, fn v -> v + 5 end, tx)
          end,
          2
        )

      assert 0 = Agent.get(commit_counter, fn v -> v end)
      assert 0 = Agent.get(verify_counter, fn v -> v end)
      assert 2 = Agent.get(abort_counter, fn v -> v end)
    end

    test "when raising AbortError" do
      {:ok, verify_counter} = Agent.start_link(fn -> 0 end)
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      StmAgent.Transaction.transaction(
        fn tx ->
          StmAgent.Transaction.on_verify(tx, fn ->
            Agent.update(verify_counter, fn v -> v + 1 end)
          end)

          StmAgent.Transaction.on_commit(tx, fn ->
            Agent.update(commit_counter, fn v -> v + 1 end)
          end)

          StmAgent.Transaction.on_abort(tx, fn ->
            Agent.update(abort_counter, fn v -> v + 1 end)
          end)

          raise StmAgent.AbortError
        end,
        1
      )

      assert 0 = Agent.get(commit_counter, fn v -> v end)
      assert 0 = Agent.get(verify_counter, fn v -> v end)
      assert 1 = Agent.get(abort_counter, fn v -> v end)
    end
  end

  describe "retry" do
    test ":aborted when hits retry limit", context do
      {:ok, counting_agent} = Agent.start_link(fn -> 0 end)
      tx2 = StmAgent.Transaction.Id.new()
      :ok = StmAgent.verify(context.agent, tx2)

      assert :aborted =
               StmAgent.Transaction.transaction(
                 fn tx ->
                   Agent.update(counting_agent, fn v -> v + 1 end)
                   StmAgent.update(context.agent, fn v -> v + 1 end, tx)
                 end,
                 5
               )

      assert 1 = StmAgent.dirty_get(context.agent, fn v -> v end)
      state = :sys.get_state(context.agent)
      assert tx2 == state.tx_verifying
      assert Enum.empty?(state.tx_data)
      assert Enum.empty?(state.tx_version)
      assert 5 = Agent.get(counting_agent, fn v -> v end)
    end

    test "keeps going with default (:infinity) retry limit", context do
      {:ok, counting_agent} = Agent.start_link(fn -> 0 end)
      tx2 = StmAgent.Transaction.Id.new()
      :ok = StmAgent.verify(context.agent, tx2)

      assert {:ok, _} =
               StmAgent.Transaction.transaction(fn tx ->
                 if Agent.get_and_update(counting_agent, fn v -> {v + 1, v + 1} end) >= 13 do
                   StmAgent.commit(context.agent, tx2)
                 end

                 StmAgent.update(context.agent, fn v -> v + 1 end, tx)
               end)

      assert 2 = StmAgent.dirty_get(context.agent, fn v -> v end)
      state = :sys.get_state(context.agent)
      assert nil == state.tx_verifying
      assert Enum.empty?(state.tx_data)
      assert Enum.empty?(state.tx_version)
      assert 13 = Agent.get(counting_agent, fn v -> v end)
    end
  end
end

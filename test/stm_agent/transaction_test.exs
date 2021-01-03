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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
        StmAgent.update(context.agent2, tx, fn v -> v + 5 end)
        StmAgent.update(context.agent, tx, fn v -> v + 3 end)
        StmAgent.update(context.agent2, tx, fn v -> v + 7 end)
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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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
        StmAgent.update(context.agent, tx, fn v -> v + 1 end)
        StmAgent.update(context.agent2, tx, fn v -> v + 7 end)
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
    :ok = StmAgent.update(context.agent, tx2, fn v -> v + 1 end)
    :ok = StmAgent.verify(context.agent, tx2)

    assert :aborted =
             StmAgent.Transaction.transaction(
               fn tx ->
                 :abort = StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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
    :ok = StmAgent.update(context.agent, tx2, fn v -> v + 1 end)
    :ok = StmAgent.verify(context.agent, tx2)

    assert :aborted =
             StmAgent.Transaction.transaction(
               fn tx ->
                 :abort = StmAgent.update(context.agent, tx, fn v -> v + 5 end)
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
                 StmAgent.update(context.agent, tx, fn v -> v + 1 end)
                 StmAgent.update(context.agent2, tx, fn v -> v + 7 end)
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
                 :ok = StmAgent.update(context.agent, tx, fn v -> v + 5 end)
                 StmAgent.dirty_update(context.agent, fn v -> v + 3 end)
               end,
               5
             )

    assert 16 = StmAgent.dirty_get(context.agent, fn v -> v end)
  end

  describe "callbacks" do
    test "StmAgent on_abort called when no update/get/etc called", context do
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)

      StmAgent.Transaction.transaction(
        fn tx ->
          StmAgent.on_abort(context.agent, tx, fn _v ->
            Agent.update(abort_counter, fn v -> v + 1 end)
          end)

          raise StmAgent.AbortError
        end,
        1
      )

      assert 1 = Agent.get(abort_counter, fn v -> v end)
    end

    test "StmAgent on_commit called when no update/get/etc called", context do
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      StmAgent.Transaction.transaction(
        fn tx ->
          StmAgent.on_commit(context.agent, tx, fn _v ->
            Agent.update(commit_counter, fn v -> v + 1 end)
          end)
        end,
        1
      )

      assert 1 = Agent.get(commit_counter, fn v -> v end)
    end

    test "committed transaction calls on_verify, on_commit", context do
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

          :ok = StmAgent.update(context.agent, tx, fn v -> v + 5 end)
        end)

      assert 1 = Agent.get(commit_counter, fn v -> v end)
      assert 1 = Agent.get(verify_counter, fn v -> v end)
      assert 0 = Agent.get(abort_counter, fn v -> v end)
    end

    test "abort calls on_abort", context do
      {:ok, verify_counter} = Agent.start_link(fn -> 0 end)
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      tx2 = StmAgent.Transaction.Id.new()
      :ok = StmAgent.update(context.agent, tx2, fn v -> v + 1 end)
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

            :abort = StmAgent.update(context.agent, tx, fn v -> v + 5 end)
          end,
          2
        )

      assert 0 = Agent.get(commit_counter, fn v -> v end)
      assert 0 = Agent.get(verify_counter, fn v -> v end)
      assert 2 = Agent.get(abort_counter, fn v -> v end)
    end

    test "raising AbortError calls on_abort" do
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

    test "raise calls on_abort" do
      {:ok, verify_counter} = Agent.start_link(fn -> 0 end)
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      try do
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

          raise "error"
        end)
      rescue
        _ -> :ok
      end

      assert 0 = Agent.get(commit_counter, fn v -> v end)
      assert 0 = Agent.get(verify_counter, fn v -> v end)
      assert 1 = Agent.get(abort_counter, fn v -> v end)
    end

    test "throw calls on_abort" do
      {:ok, verify_counter} = Agent.start_link(fn -> 0 end)
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      try do
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

          throw("error")
        end)
      catch
        _ -> :ok
      end

      assert 0 = Agent.get(commit_counter, fn v -> v end)
      assert 0 = Agent.get(verify_counter, fn v -> v end)
      assert 1 = Agent.get(abort_counter, fn v -> v end)
    end

    test "exit calls on_abort" do
      {:ok, verify_counter} = Agent.start_link(fn -> 0 end)
      {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
      {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

      try do
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

          exit("error")
        end)
      catch
        :exit, _ -> :ok
      end

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
                   StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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

                 StmAgent.update(context.agent, tx, fn v -> v + 1 end)
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

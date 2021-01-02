defmodule StmAgent.StateTest do
  use ExUnit.Case

  describe "get" do
    test "sets version" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, 11, state} = StmAgent.State.get(state, tx, fn v -> v + 10 end)

      assert Map.get(state.tx_version, tx) == 1
    end

    test ":abort when other verifying tx modified data on 1st access" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx2)

      assert {:abort, state} = StmAgent.State.get(state, tx1, fn v -> v + 10 end)
      assert state.version == Map.get(state.tx_version, tx1)
    end

    test ":abort when other verifying tx modified data on 2nd access" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, _value, state} = StmAgent.State.get(state, tx1, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx2)

      assert {:abort, state} = StmAgent.State.get(state, tx1, fn v -> v + 10 end)
      assert state.version == Map.get(state.tx_version, tx1)
    end

    test ":ok when other verifying tx did not modify data" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, _value, state} = StmAgent.State.get(state, tx1, fn v -> v + 10 end)
      {:ok, _value, state} = StmAgent.State.get(state, tx2, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx2)

      assert {:ok, _value, _state} = StmAgent.State.get(state, tx1, fn v -> v + 10 end)
    end

    test ":abort when StmAgent.AbortError raised" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      assert {:abort, _state} =
               StmAgent.State.get(state, tx, fn _v -> raise StmAgent.AbortError end)
    end
  end

  describe "update" do
    test "sets tx version and tx data" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx, fn v -> v + 10 end)

      assert state.data == 1
      assert Map.get(state.tx_version, tx) == 1
      assert Map.get(state.tx_data, tx) == 11
    end

    test ":abort when StmAgent.AbortError raised" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      assert {:abort, _state} =
               StmAgent.State.update(state, tx, fn _v -> raise StmAgent.AbortError end)
    end
  end

  describe "get_and_update" do
    test "sets tx version and tx data" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, 4, state} = StmAgent.State.get_and_update(state, tx, fn v -> {v + 3, v + 10} end)

      assert state.data == 1
      assert Map.get(state.tx_version, tx) == 1
      assert Map.get(state.tx_data, tx) == 11
    end

    test ":abort when StmAgent.AbortError raised" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      assert {:abort, _state} =
               StmAgent.State.get_and_update(state, tx, fn _v -> raise StmAgent.AbortError end)
    end
  end

  describe "dirty_get" do
    test "basic" do
      state = StmAgent.State.new(1)

      assert 2 == StmAgent.State.dirty_get(state, fn v -> v + 1 end)
    end

    test "works while verifying" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()
      {:ok, state} = StmAgent.State.verify(state, tx)

      assert 2 == StmAgent.State.dirty_get(state, fn v -> v + 1 end)
    end
  end

  describe "dirty_update" do
    test "basic" do
      state = StmAgent.State.new(1)

      assert {:ok, state} = StmAgent.State.dirty_update(state, fn v -> v + 5 end, self())
      assert 6 = state.data
      assert 2 = state.version
    end

    test "retry when verifying" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()
      {:ok, state} = StmAgent.State.verify(state, tx)

      assert {:retry, state} = StmAgent.State.dirty_update(state, fn v -> v + 5 end, 1)
      assert {:retry, state} = StmAgent.State.dirty_update(state, fn v -> v + 5 end, 7)
      assert {:retry, state} = StmAgent.State.dirty_update(state, fn v -> v + 5 end, 3)

      assert 1 = state.data
      assert 1 = state.version
      assert [3, 7, 1] = state.retry_queue
    end
  end

  describe "dirty_get_and_update" do
    test "basic" do
      state = StmAgent.State.new(1)

      assert {:ok, 3, state} =
               StmAgent.State.dirty_get_and_update(state, fn v -> {v + 2, v + 5} end, self())

      assert 6 = state.data
      assert 2 = state.version
    end

    test "retry when verifying" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()
      {:ok, state} = StmAgent.State.verify(state, tx)

      assert {:retry, state} = StmAgent.State.dirty_get_and_update(state, fn v -> v + 5 end, 1)
      assert {:retry, state} = StmAgent.State.dirty_get_and_update(state, fn v -> v + 5 end, 7)
      assert {:retry, state} = StmAgent.State.dirty_get_and_update(state, fn v -> v + 5 end, 3)

      assert 1 = state.data
      assert 1 = state.version
      assert [3, 7, 1] = state.retry_queue
    end
  end

  describe "retry queue" do
    test "clear_retry_queue clears it out" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()
      {:ok, state} = StmAgent.State.verify(state, tx)

      {:retry, state} = StmAgent.State.dirty_update(state, fn v -> v + 5 end, 1)
      {:retry, state} = StmAgent.State.dirty_update(state, fn v -> v + 5 end, 7)
      {:retry, state} = StmAgent.State.dirty_update(state, fn v -> v + 5 end, 3)

      {state, retry_queue} = StmAgent.State.clear_retry_queue(state)

      assert [] = state.retry_queue
      assert [1, 7, 3] = retry_queue
    end
  end

  describe "verify" do
    test "basic" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx)

      assert tx == state.tx_verifying
    end

    test ":ok when double verify" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx)
      {:ok, state} = StmAgent.State.verify(state, tx)

      assert tx == state.tx_verifying
    end

    test ":ok when no previous access" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.verify(state, tx)

      assert tx == state.tx_verifying
    end

    test ":abort when other tx verified modified data" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx1, fn v -> v + 10 end)
      {:ok, 2, state} = StmAgent.State.get(state, tx2, fn v -> v + 1 end)
      {:ok, state} = StmAgent.State.verify(state, tx1)

      assert :abort = StmAgent.State.verify(state, tx2)
    end

    test ":abort when other tx verified non-modified data" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, 11, state} = StmAgent.State.get(state, tx1, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 1 end)
      {:ok, state} = StmAgent.State.verify(state, tx1)

      assert :abort = StmAgent.State.verify(state, tx2)
    end

    test ":abort when other transaction committed modified data" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx1, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 1 end)
      {:ok, state} = StmAgent.State.verify(state, tx1)
      {:ok, state} = StmAgent.State.commit(state, tx1)

      assert :abort = StmAgent.State.verify(state, tx2)
    end
  end

  describe "abort" do
    test "when verifying self" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx)
      state = StmAgent.State.abort(state, tx)

      assert 1 = state.version
      assert 1 = state.data
      assert nil == state.tx_verifying
      assert !Map.has_key?(state.tx_data, tx)
      assert !Map.has_key?(state.tx_version, tx)
    end

    test "doesn't delete other tx data" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx1, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 10 end)
      state = StmAgent.State.abort(state, tx1)

      assert Map.has_key?(state.tx_data, tx2)
      assert Map.has_key?(state.tx_version, tx2)
    end

    test "when not verifying" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx, fn v -> v + 10 end)
      state = StmAgent.State.abort(state, tx)

      assert 1 = state.version
      assert 1 = state.data
      assert nil == state.tx_verifying
      assert !Map.has_key?(state.tx_data, tx)
      assert !Map.has_key?(state.tx_version, tx)
    end

    test "when verifying other" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx1, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx2)
      state = StmAgent.State.abort(state, tx1)

      assert 1 = state.version
      assert 1 = state.data
      assert tx2 == state.tx_verifying
      assert !Map.has_key?(state.tx_data, tx1)
      assert !Map.has_key?(state.tx_version, tx1)
      assert Map.has_key?(state.tx_data, tx2)
      assert Map.has_key?(state.tx_version, tx2)
    end
  end

  describe "commit" do
    test "basic" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx)
      {:ok, state} = StmAgent.State.commit(state, tx)

      assert 2 = state.version
      assert 11 = state.data
      assert !Map.has_key?(state.tx_data, tx)
      assert !Map.has_key?(state.tx_version, tx)
    end

    test "doesn't delete other tx data" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx1, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 5 end)
      {:ok, state} = StmAgent.State.verify(state, tx1)
      {:ok, state} = StmAgent.State.commit(state, tx1)

      assert Map.has_key?(state.tx_data, tx2)
      assert Map.has_key?(state.tx_version, tx2)
    end

    test "get doesn't cause version increment" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, 11, state} = StmAgent.State.get(state, tx, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.verify(state, tx)
      {:ok, state} = StmAgent.State.commit(state, tx)

      assert 1 == state.version
    end

    test "noop doesn't cause version increment" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.verify(state, tx)
      {:ok, state} = StmAgent.State.commit(state, tx)

      assert 1 == state.version
    end

    test ":error when not verified" do
      state = StmAgent.State.new(1)
      tx = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx, fn v -> v + 10 end)

      assert :error = StmAgent.State.commit(state, tx)
    end

    test ":error when verified by other tx" do
      state = StmAgent.State.new(1)
      tx1 = StmAgent.Transaction.Id.new()
      tx2 = StmAgent.Transaction.Id.new()

      {:ok, state} = StmAgent.State.update(state, tx1, fn v -> v + 10 end)
      {:ok, state} = StmAgent.State.update(state, tx2, fn v -> v + 5 end)
      {:ok, state} = StmAgent.State.verify(state, tx1)

      assert :error = StmAgent.State.commit(state, tx2)
    end
  end

  test "on_commit stores function" do
    state = StmAgent.State.new(1)
    tx = StmAgent.Transaction.Id.new()

    state = StmAgent.State.on_commit(state, tx, fn _v -> :ok end)
    assert 1 == Enum.count(Map.get(state.tx_on_commit, tx))

    state = StmAgent.State.on_commit(state, tx, fn _v -> :ok end)
    assert 2 == Enum.count(Map.get(state.tx_on_commit, tx))
  end

  test "on_abort stores function" do
    state = StmAgent.State.new(1)
    tx = StmAgent.Transaction.Id.new()

    state = StmAgent.State.on_abort(state, tx, fn _v -> :ok end)
    assert 1 == Enum.count(Map.get(state.tx_on_abort, tx))

    state = StmAgent.State.on_abort(state, tx, fn _v -> :ok end)
    assert 2 == Enum.count(Map.get(state.tx_on_abort, tx))
  end

  describe "callbacks" do
    setup [:setup_callbacks]

    test "commit", context do
      state = context.state
      {:ok, state} = StmAgent.State.verify(state, context.tx1)
      {:ok, state} = StmAgent.State.commit(state, context.tx1)

      assert 1 == Agent.get(context.commit_counter, fn v -> v end)
      assert 0 == Agent.get(context.abort_counter, fn v -> v end)
      assert 1 == Enum.count(state.tx_on_commit)
      assert 1 == Enum.count(state.tx_on_abort)

      {:ok, state} = StmAgent.State.verify(state, context.tx2)
      {:ok, state} = StmAgent.State.commit(state, context.tx2)

      assert 15 == Agent.get(context.commit_counter, fn v -> v end)
      assert 0 == Agent.get(context.abort_counter, fn v -> v end)
      assert Enum.empty?(state.tx_on_commit)
      assert Enum.empty?(state.tx_on_abort)
    end

    test "abort", context do
      state = context.state

      state = StmAgent.State.abort(state, context.tx1)

      assert 0 == Agent.get(context.commit_counter, fn v -> v end)
      assert 5 == Agent.get(context.abort_counter, fn v -> v end)
      assert 1 == Enum.count(state.tx_on_commit)
      assert 1 == Enum.count(state.tx_on_abort)

      state = StmAgent.State.abort(state, context.tx2)

      assert 0 == Agent.get(context.commit_counter, fn v -> v end)
      assert 25 == Agent.get(context.abort_counter, fn v -> v end)
      assert Enum.empty?(state.tx_on_commit)
      assert Enum.empty?(state.tx_on_abort)
    end
  end

  defp setup_callbacks(_context) do
    state = StmAgent.State.new(1)

    {:ok, abort_counter} = Agent.start_link(fn -> 0 end)
    {:ok, commit_counter} = Agent.start_link(fn -> 0 end)

    tx1 = StmAgent.Transaction.Id.new()
    tx2 = StmAgent.Transaction.Id.new()

    state =
      StmAgent.State.on_commit(state, tx1, fn _v ->
        Agent.update(commit_counter, fn v -> v + 1 end)
      end)

    state =
      StmAgent.State.on_commit(state, tx2, fn _v ->
        Agent.update(commit_counter, fn v -> v + 3 end)
      end)

    state =
      StmAgent.State.on_commit(state, tx2, fn _v ->
        Agent.update(commit_counter, fn v -> v + 11 end)
      end)

    state =
      StmAgent.State.on_abort(state, tx1, fn _v ->
        Agent.update(abort_counter, fn v -> v + 5 end)
      end)

    state =
      StmAgent.State.on_abort(state, tx2, fn _v ->
        Agent.update(abort_counter, fn v -> v + 7 end)
      end)

    state =
      StmAgent.State.on_abort(state, tx2, fn _v ->
        Agent.update(abort_counter, fn v -> v + 13 end)
      end)

    [
      state: state,
      tx1: tx1,
      tx2: tx2,
      abort_counter: abort_counter,
      commit_counter: commit_counter
    ]
  end
end

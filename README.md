# StmAgent

Software transactional memory for Elixir.  

Users beware: I'm new to Elixir and have never used this on a real project.

# Example
```elixir
{:ok, counter1} = StmAgent.start_link(fn -> 1 end)
{:ok, counter2} = StmAgent.start_link(fn -> 1 end)

# StmAgent.Transaction.transaction will automatically retry failed commits
StmAgent.Transaction.transaction(fn tx ->
  counter1_value = StmAgent.get!(counter1, tx, fn v -> v end)
  StmAgent.update!(counter2, tx, fn v -> v + 2 * counter1_value end)

  # Only called on successful commit
  StmAgent.on_commit(counter2, tx, fn new_value ->
    IO.puts("Successfully updated value to #{new_value}")
  end)
end)
```

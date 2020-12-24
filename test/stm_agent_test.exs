defmodule StmAgentTest do
  use ExUnit.Case
  doctest StmAgent

  test "greets the world" do
    assert StmAgent.hello() == :world
  end
end

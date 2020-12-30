defmodule StmAgent.AbortError do
  @moduledoc """
  Used to signal an abort in ! functions.
  """

  defexception message: "Transaction abort"
end

defmodule GenQueue.Adapters.VerkMockTest do
  use ExUnit.Case

  import GenQueue.Test

  defmodule Enqueuer do
    Application.put_env(:gen_queue_verk, __MODULE__, adapter: GenQueue.Adapters.VerkMock)

    use GenQueue, otp_app: :gen_queue_verk
  end

  setup do
    setup_test_queue(Enqueuer)
  end

  describe "push/2" do
    test "sends the job back to the registered process from module" do
      {:ok, _} = Enqueuer.push(Job)
      assert_receive({Job, [], %{jid: _}})
    end

    test "sends the job back to the registered process from module tuple" do
      {:ok, _} = Enqueuer.push({Job})
      assert_receive({Job, [], %{jid: _}})
    end

    test "sends the job back to the registered process from module and args" do
      {:ok, _} = Enqueuer.push({Job, ["foo", "bar"]})
      assert_receive({Job, ["foo", "bar"], %{jid: _}})
    end

    test "sends the job back to the registered process with :at delay" do
      {:ok, _} = Enqueuer.push({Job, []}, [at: DateTime.utc_now()])
      assert_receive({Job, [], %{at: _, jid: _}})
    end

    test "does nothing if process is not registered" do
      reset_test_queue(Enqueuer)
      {:ok, _} = Enqueuer.push(Job)
    end
  end
end

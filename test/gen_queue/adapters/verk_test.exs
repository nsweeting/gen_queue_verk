defmodule GenQueue.Adapters.VerkTest do
  use ExUnit.Case

  import GenQueue.Test
  import GenQueue.VerkTestHelpers

  defmodule Enqueuer do
    Application.put_env(:gen_queue_verk, __MODULE__, adapter: GenQueue.Adapters.Verk)

    use GenQueue, otp_app: :gen_queue_verk
  end
  
  defmodule Job do
    def perform do
      send_item(Enqueuer, :performed)
    end
  
    def perform(arg1) do
      send_item(Enqueuer, {:performed, arg1})
    end

    def perform(arg1, arg2) do
      send_item(Enqueuer, {:performed, arg1, arg2})
    end
  end

  setup_all do
    Application.put_env(:verk, :redis_url, "redis://127.0.0.1:6379")
    Application.put_env(:verk, :queues, [default: 1, other: 1])
  end

  setup do
    setup_global_test_queue(Enqueuer, :test)
  end

  describe "push/2" do
    test "enqueues and runs job from module" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push(Job)
      assert_receive(:performed)
      assert {Job, [], %{queue: "default"}} = job
      stop_process(pid)
    end
  
    test "enqueues and runs job from module tuple" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job})
      assert_receive(:performed)
      assert {Job, [], %{queue: "default", jid: _}} = job
      stop_process(pid)
    end

    test "enqueues and runs job from module and args" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job, ["foo", "bar"]})
      assert_receive({:performed, "foo", "bar"})
      assert {Job, ["foo", "bar"], %{queue: "default", jid: _}} = job
      stop_process(pid)
    end

    test "enqueues a job with :at delay" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job, ["foo"]}, [at: DateTime.utc_now()])
      assert_receive({:performed, "foo"})
      assert {Job, ["foo"], %{queue: "default", jid: _, at: _}} = job
      stop_process(pid)
    end

    test "enqueues a job to a specific queue" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job1} = Enqueuer.push({Job, [1]}, [queue: "default"])
      {:ok, job2} = Enqueuer.push({Job, [2]}, [queue: "other"])
      assert_receive({:performed, 1})
      assert_receive({:performed, 2})
      assert {Job, [1], %{queue: "default", jid: _}} = job1
      assert {Job, [2], %{queue: "other", jid: _}} = job2
      stop_process(pid)
    end
  end
end

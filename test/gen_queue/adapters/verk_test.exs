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
    Application.put_env(:verk, :queues, default: 1, other: 1)
  end

  setup do
    setup_global_test_queue(Enqueuer, :test)
  end

  describe "push/2" do
    test "enqueues and runs job from module" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push(Job)
      assert_receive(:performed)
      assert %GenQueue.Job{module: Job, args: [], queue: "default"} = job
      stop_process(pid)
    end

    test "enqueues and runs job from module tuple" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job})
      assert_receive(:performed)
      assert %GenQueue.Job{module: Job, args: [], queue: "default"} = job
      stop_process(pid)
    end

    test "enqueues and runs job from module and args" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job, ["foo", "bar"]})
      assert_receive({:performed, "foo", "bar"})
      assert %GenQueue.Job{module: Job, args: ["foo", "bar"], queue: "default"} = job
      stop_process(pid)
    end

    test "enqueues and runs job from module and single arg" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job, "foo"})
      assert_receive({:performed, "foo"})
      assert %GenQueue.Job{module: Job, args: ["foo"], queue: "default"} = job
      stop_process(pid)
    end

    test "enqueues a job with datetime delay" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job, ["foo"]}, delay: DateTime.utc_now())
      assert_receive({:performed, "foo"})
      assert %GenQueue.Job{module: Job, args: ["foo"], queue: "default", delay: %DateTime{}} = job
      stop_process(pid)
    end

    test "enqueues a job with millisecond delay" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job} = Enqueuer.push({Job, ["foo"]}, delay: 0)
      assert_receive({:performed, "foo"})
      assert %GenQueue.Job{module: Job, args: ["foo"], queue: "default", delay: delay} = job
      assert is_integer(delay)
      stop_process(pid)
    end

    test "enqueues a job to a specific queue" do
      {:ok, pid} = Enqueuer.start_link()
      {:ok, job1} = Enqueuer.push({Job, [1]}, queue: "default")
      {:ok, job2} = Enqueuer.push({Job, [2]}, queue: "other")
      assert_receive({:performed, 1})
      assert_receive({:performed, 2})
      assert %GenQueue.Job{module: Job, args: [1], queue: "default"} = job1
      assert %GenQueue.Job{module: Job, args: [2], queue: "other"} = job2
      stop_process(pid)
    end
  end

  test "enqueuer can be started as part of a supervision tree" do
    {:ok, pid} = Supervisor.start_link([{Enqueuer, []}], strategy: :one_for_one)
    {:ok, job} = Enqueuer.push(Job)
    assert_receive(:performed)
    stop_process(pid)
  end
end

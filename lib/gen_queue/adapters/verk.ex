defmodule GenQueue.Adapters.Verk do
  @moduledoc """
  An adapter for `GenQueue` to enable functionaility with `Verk`.
  """

  use GenQueue.JobAdapter

  def start_link(_gen_queue, _opts) do
    Verk.Supervisor.start_link()
  end

  @doc """
  Push a `GenQueue.Job` for `Verk` to consume.

  ## Parameters:
    * `gen_queue` - A `GenQueue` module
    * `job` - A `GenQueue.Job`

  ## Returns:
    * `{:ok, job}` if the operation was successful
    * `{:error, reason}` if there was an error
  """
  @spec handle_job(gen_queue :: GenQueue.t(), job :: GenQueue.Job.t()) ::
          {:ok, GenQueue.Job.t()} | {:error, any}
  def handle_job(gen_queue, %GenQueue.Job{queue: nil} = job) do
    handle_job(gen_queue, %{job | queue: "default"})
  end

  def handle_job(_gen_queue, %GenQueue.Job{delay: %DateTime{} = delay} = job) do
    case job |> to_verk_job() |> Verk.schedule(delay) do
      {:ok, _} -> {:ok, job}
      error -> error
    end
  end

  def handle_job(_gen_queue, %GenQueue.Job{delay: offset} = job) when is_integer(offset) do
    delay = (:os.system_time(:seconds) + round(offset / 1000)) |> DateTime.from_unix!()
    case job |> to_verk_job() |> Verk.schedule(delay) do
      {:ok, _} -> {:ok, job}
      error -> error
    end
  end

  def handle_job(_gen_queue, job) do
    case job |> to_verk_job() |> Verk.enqueue() do
      {:ok, _} -> {:ok, job}
      error -> error
    end
  end

  defp to_verk_job(job) do
    struct(Verk.Job, [class: job.module, args: job.args, queue: job.queue])
  end
end

import asyncio
import sys

ARGV = dict(enumerate(sys.argv))
ARTIFACT = ARGV.get(1, "./target/artifact/simple-entropy")
PEER_HOSTS = ["nsl-node1.d2"] * 40
WORK_DIR = "/local/cowsay/artifacts"
PLAZA = "http://nsl-node1.d2:8080"


async def upload_artifact():
    tasks = []
    for host in set(PEER_HOSTS):
        proc = await asyncio.create_subprocess_shell(
            f"rsync {ARTIFACT} {host}:{WORK_DIR}/entropy"
        )
        tasks.append(proc.wait())
    codes = await asyncio.gather(*tasks)
    assert all(result == 0 for result in codes)


async def run_peers():
    tasks = []
    for index, host in enumerate(PEER_HOSTS):
        proc = await asyncio.create_subprocess_shell(
            f"ssh {host}"
            " RUST_LOG=info RUST_BACKTRACE=1"
            f" {WORK_DIR}/entropy {host} --plaza {PLAZA}"
            f" 1> {WORK_DIR}/entropy-{index:03}-output.txt"
            f" 2> {WORK_DIR}/entropy-{index:03}-errors.txt"
        )

        async def wait(proc, index, host):
            code = await proc.wait()
            return code, index, host

        tasks.append(wait(proc, index, host))
    active_shutdown = False
    while tasks:
        done_tasks, tasks = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for done_task in done_tasks:
            code, index, host = done_task.result()
            if code != 0:
                print(f"peer {index} on {host} crashed ({code})")
                if not active_shutdown:
                    asyncio.create_task(shutdown_peers())
                    active_shutdown = True
    return active_shutdown


async def shutdown_peers():
    proc = await asyncio.create_subprocess_shell(f"curl -X POST {PLAZA}/shutdown")
    await proc.wait()


async def main():
    print("upload artifact")
    await upload_artifact()
    print("run peers")
    if await run_peers():
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())

import types
from importlib.machinery import SourceFileLoader
from pathlib import Path


THIS_DIR = Path(__file__).parent


async def test_run_job_burst(loop, caplog):
    loader = SourceFileLoader("demo", str(THIS_DIR / "../docs/examples/main_demo.py"))
    demo = types.ModuleType(loader.name)
    loader.exec_module(demo)
    # pylint: disable=no-member
    worker = demo.Worker(burst=True, loop=loop)

    # pylint: disable=no-member
    downloader = demo.Downloader(loop=loop)

    await downloader.download_content("http://example.com")
    await worker.run()
    await downloader.close()
    assert any(
        "Downloader.download_content(http://example.com)" in m for m in caplog.messages
    )
    assert any("Downloader.download_content ● 1" in m for m in caplog.messages)

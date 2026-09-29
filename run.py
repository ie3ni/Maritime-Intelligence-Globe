import os
import subprocess
import sys
import threading
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))


def open_application(url: str) -> None:
    """Open the UI in an optional project-selected browser."""
    preferred_browser = os.getenv("MARITIME_BROWSER")
    if sys.platform == "darwin" and preferred_browser:
        result = subprocess.run(
            ["open", "-a", preferred_browser, url],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            return
    webbrowser.open(url)


if __name__ == "__main__":
    import uvicorn
    from maritime.config import load_config
    from maritime.storage.database import MaritimeDatabase

    cfg = load_config(ROOT)
    db = MaritimeDatabase(cfg)
    status = db.status()
    print("\nMaritime Intelligence Globe")
    print(f"Database: {cfg.database_path}")
    print(f"Maritime data: {status.get('earliest_available_date') or 'not bootstrapped'} → {status.get('latest_available_date') or 'not bootstrapped'}")
    print(f"Ports: {status.get('port_count', 0)}")
    print(f"Application: http://{cfg.host}:{cfg.port}\n")
    if os.getenv("MARITIME_OPEN_BROWSER") == "1":
        browser_timer = threading.Timer(1.0, open_application, args=(f"http://{cfg.host}:{cfg.port}",))
        browser_timer.daemon = True
        browser_timer.start()
    uvicorn.run("maritime.api.main:app", host=cfg.host, port=cfg.port, reload=False)

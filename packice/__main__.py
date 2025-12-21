import os


def main() -> None:
    # Imported lazily to avoid requiring optional dependencies for both versions.
    version = os.getenv("PACKICE_VERSION", "v1")
    if version == "v0":
        from packice.v0.server import run as run_v0  # type: ignore

        run_v0()
    else:
        from packice.v1.node import run as run_v1  # type: ignore

        run_v1()


if __name__ == "__main__":
    main()

from future import standard_library

standard_library.install_aliases()
from mozart import app


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888, debug=True, processes=2)

# Socket Server

![Build Status](https://github.com/tutorcruncher/socket-server/workflows/CI/badge.svg)
[![codecov](https://codecov.io/gh/tutorcruncher/socket-server/branch/master/graph/badge.svg)](https://codecov.io/gh/tutorcruncher/socket-server)

Backend application for [TutorCruncher's](https://tutorcruncher.com) web integration.
## Setup and Run

To set up and run this project, follow these steps:

1. **Clone the repository:**
   ```sh
   git clone git@github.com:tutorcruncher/socket-server.git
   cd socket-server
   ```

2. **Install dependencies:**
   ```sh
   make install
   ```

3. **Reset the database:**
  ```sh
  make reset-db
  ```

3. **Run the application:**
   ```sh
   python tcsocket/run.py auto
   ```
   
**Note:** You might have to run this with `sudo` if you are not in the `docker` group.

## Environment Variables

The environment variables for this project are:

- `BIND_IP`: The IP address to bind the web server to. Default is `127.0.0.1`.
- `PORT`: The port number to bind the web server to. Default is `8000`.
- `DYNO`: Used to infer whether to run the web server or worker. If it starts with `web`, the web server will run; otherwise, the worker will run.
- `DATABASE_URL`: The URL for the database connection.
- `REDIS_URL`: The URL for the Redis connection.

You can set these environment variables in your shell or in a `.env` file. Here is an example of how to set them in a `.env` file:

```sh
BIND_IP=127.0.0.1
PORT=8000
DYNO=web.1
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/socket_test
REDIS_URL=redis://localhost:6379/0
```

## Commands

- **Run the application:**
  ```sh
  python tcsocket/run.py auto
  ```

- **Reset the database:**
  ```sh
  make reset-db
  ```

- **Open an IPython shell:**
  ```sh
  python tcsocket/run.py shell
  ```

- **Run a patch script:**
  ```sh
  python tcsocket/run.py patch --live <patch_name>
  ```
- **Format the code:**
   ```sh
   make format
   ```

- **Lint the code:**
   ```sh
   make lint
   ```

- **Run tests:**
   ```sh
   make test
   ```

## Docker

The project includes a `Dockerfile` for building a Docker image. To build the Docker image, run:

```sh
make build
```

## Deployment

To deploy the project to Heroku, use the following command:

```sh
make prod-push
```

or

To deploy socket-server, please create a new tag/release, then run the following command:

```
git push heroku master
```

**Make sure you have checked out master and pulled all the recent changes.**

## License

Copyright TutorCruncher ltd. 2017 - 2022.
All rights reserved.

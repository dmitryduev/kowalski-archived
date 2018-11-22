# Kowalski 1.0

The legendary penguin got `docker`ized! Under the hood, it switched from `flask` to `aiohttp` and ditched 
the task queue to improve performance and robustness.

## Production service  

### Set-up instructions

#### Pre-requisites

Clone the repo and cd to the cloned directory:
```bash
git clone https://github.com/dmitryduev/kowalski.git
cd kowalski
```

Create `secrets.json` with confidential/secret data:
```json
{
  "server" : {
    "SECRET_KEY": "secret_key",
    "JWT_SECRET_KEY": "jwt_secret_key",
    "admin_username": "ADMIN",
    "admin_password": "PASSWORD"
  },
  "database": {
    "admin": "mongoadmin",
    "admin_pwd": "mongoadminsecret",
    "user": "user",
    "pwd": "pwd"
  },
  "kafka": {
    "bootstrap.servers": "IP1:PORT,IP2:PORT",
    "default.topic.config": {
      "auto.offset.reset": "earliest"
    },
    "group": "kowalski.caltech.edu",
    "cmd": {
      "zookeeper": "IP:PORT"
    }
  }
}
```

#### Using `docker-compose` (for production)

Change `kowalski.caltech.edu` in `docker-compose.yml` and in `traefik/traefik.toml` to your domain. 

Run `docker-compose` to start the service:
```bash
docker-compose up --build -d
```

To tear everything down (i.e. stop and remove the containers), run:
```bash
docker-compose down
```

---

#### Using plain `Docker` (for dev/testing)

If you want to use `docker run` instead:

Create a persistent Docker volume for MongoDB and to store data:
```bash
docker volume create kowalski_mongodb
docker volume create kowalski_data
```

Launch the MongoDB container. Feel free to change u/p for the admin, 
but make sure to change `secrets.json` and `docker-compose.yml` correspondingly.
```bash
docker run -d --restart always --name kowalski-mongo -p 27023:27017 -v kowalski_mongodb:/data/db \
       -e MONGO_INITDB_ROOT_USERNAME=mongoadmin -e MONGO_INITDB_ROOT_PASSWORD=mongoadminsecret \
       mongo:latest
```

Build and launch the app container:
```bash
docker build --rm -t kowalski:latest -f Dockerfile .
docker run --name kowalski -d --restart always -p 8000:4000 -v kowalski_data:/data -v /path/to/tmp:/_tmp --link kowalski-mongo:mongo kowalski:latest
# test mode:
docker run -it --rm --name kowalski -p 8000:4000 -v kowalski_data:/data -v /path/to/tmp:/_tmp --link kowalski-mongo:mongo kowalski:latest

```

`Kowalski` will be available on port 8000 of the `Docker` host machine. 


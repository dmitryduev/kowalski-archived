# Kowalski 2.0

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

Create `secrets.json` with secret info:
```json
{
  "server" : {
    "SECRET_KEY": "very_secret_key",
    "JWT_SECRET_KEY": "even_more_secret_key"
  },
  "database": {
    "admin_username": "ADMIN",
    "admin_password": "PASSWORD"
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
docker volume create kowalski-mongo-volume
docker volume create kowalski-volume
```

Launch the MongoDB container. Feel free to change u/p for the admin, 
but make sure to change `config.json` correspondingly.
```bash
docker run -d --restart always --name kowalski-mongo -p 27023:27017 -v kowalski-mongo-volume:/data/db \
       -e MONGO_INITDB_ROOT_USERNAME=mongoadmin -e MONGO_INITDB_ROOT_PASSWORD=mongoadminsecret \
       mongo:latest
```

Build and launch the app container:
```bash
docker build --rm -t kowalski:latest -f Dockerfile .
docker run --name kowalski -d --restart always -p 8000:4000 -v kowalski-volume:/data --link kowalski-mongo:mongo kowalski:latest
# test mode:
docker run -it --rm --name deep-asteroids -p 8000:4000 -v kowalski-volume:/data --link kowalski-mongo:mongo kowalski:latest

```

`Kowalski` will be available on port 8000 of the `Docker` host machine. 


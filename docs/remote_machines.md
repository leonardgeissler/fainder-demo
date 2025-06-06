# Remote machines

### With docker

To run the `ui` and `backend` on separate machines, you start the `ui` on your local machine:

```bash
docker compose up ui
```

Then you connect to a remote machine while forwarding the port `8000`:

```bash
ssh -L 8000:localhost:8000 user@example.com
```

Alternatively, you can define the LocalForward in your ssh config:

```
Host Example
    HostName example.com
    User user
    LocalForward 8000 localhost:8000
```

On that machine, you then start the backend with:

```bash
docker compose up backend
```

While the ssh connection remains intact, the backend and `ui` will be able to communicate.

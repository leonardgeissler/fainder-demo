# Remote Backend Setup

## SSH Port Forwarding

To run the `ui` and `backend` on separate machines, you can use SSH port forwarding to connect them.
This allows you to access the backend service running on a remote machine from your local machine.

To run the `ui` and `backend` on separate machines, first start the `ui` on your local machine:

```bash
docker compose up ui
```

Then, connect to a remote machine via SSH while forwarding the port `8000`:

```bash
ssh -L 8000:localhost:8000 user@example.com
```

Alternatively, you can define the `LocalForward` in your SSH config:

```bash
# ~/.ssh/config
Host Example
    HostName example.com
    User user
    LocalForward 8000 localhost:8000
```

Finally, start the `backend` component on the remote machine.
Make sure that your dataset collection is available on the remote machine and that you set all necessary environment variables.

```bash
docker compose up backend
```

While the SSH connection remains intact, the backend and UI will be able to communicate.

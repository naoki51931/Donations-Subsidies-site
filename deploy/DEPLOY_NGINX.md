# Nginx Reverse Proxy Setup (Flask + Gunicorn)

## 1. Install dependencies in venv

```bash
cd /home/ubuntu/taichi_support_donation_site02/donation
source venv/bin/activate
pip install -r requirements.txt
```

## 2. Install and register systemd service

```bash
sudo cp deploy/systemd/donation.service /etc/systemd/system/donation.service
sudo systemctl daemon-reload
sudo systemctl enable donation
sudo systemctl restart donation
sudo systemctl status donation --no-pager
```

## 3. Install nginx site config

```bash
sudo cp deploy/nginx/donation.conf /etc/nginx/sites-available/donation.conf
sudo ln -sf /etc/nginx/sites-available/donation.conf /etc/nginx/sites-enabled/donation.conf
sudo nginx -t
sudo systemctl reload nginx
```

## 4. Verify

```bash
curl -I http://127.0.0.1/
```

If you use a domain, replace `server_name _;` in `deploy/nginx/donation.conf` with your domain.

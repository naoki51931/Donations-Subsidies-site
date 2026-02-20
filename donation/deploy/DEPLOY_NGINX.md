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
sudo mkdir -p /var/www/certbot
sudo nginx -t
sudo systemctl reload nginx
```

## 4. Issue SSL certificate (Let's Encrypt)

```bash
sudo certbot certonly --webroot \
  -w /var/www/certbot \
  -d kifukin.support-home.org \
  -d support-home.org \
  -d www.support-home.org
```

After certificate issuance, run:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 5. Verify

```bash
curl -I http://127.0.0.1/
curl -I http://kifukin.support-home.org/
curl -I https://kifukin.support-home.org/
```

Expected:
- `http://...` returns `301` redirect to `https://...`
- `https://...` returns `200` (or app-specific response)

If you use another domain, update `server_name` and the `ssl_certificate` / `ssl_certificate_key` paths in `deploy/nginx/donation.conf`.

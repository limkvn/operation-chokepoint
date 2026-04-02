# Publishing a Dashboard to GitHub Pages

This documents the deployment setup used for the shipping nowcast dashboard, which serves a single self-contained HTML file via GitHub Pages.

## Overview

The approach is simple: a dedicated GitHub repository holds a single `index.html` file on the `main` branch. GitHub Pages serves it as a static site. There is no build step on GitHub's side — the HTML file is generated locally (or via automation) and pushed directly.

## One-Time Setup

### 1. Create the GitHub repository

```bash
gh repo create your-username/your-dashboard --public --clone
cd your-dashboard
```

Or create it manually on github.com, then clone:

```bash
git clone https://github.com/your-username/your-dashboard.git
cd your-dashboard
```

### 2. Enable GitHub Pages

Go to **Settings > Pages** in the repository, then:

- **Source**: Deploy from a branch
- **Branch**: `main`
- **Folder**: `/ (root)`
- Click **Save**

The site will be live at `https://your-username.github.io/your-dashboard/` within a minute or two.

### 3. Push your first HTML file

```bash
cp /path/to/your/generated_dashboard.html index.html
git add index.html
git commit -m "Initial dashboard"
git push origin main
```

GitHub Pages looks for `index.html` at the root of the branch — that's the only filename convention that matters.

## Ongoing Deployment

Each time you regenerate the dashboard:

```bash
# 1. Generate the HTML (your local build step)
python scripts/build_nowcast_dashboard.py

# 2. Copy into the deploy repo
cp outputs/nowcast/hormuz_nowcast_dashboard.html /path/to/your-dashboard/index.html

# 3. Commit and push
cd /path/to/your-dashboard
git add index.html
git commit -m "Update dashboard — $(date +%Y-%m-%d)"
git push origin main
```

GitHub Pages typically updates within 30-60 seconds of the push.

## What We Used for the Shipping Nowcast

- **Repository**: `https://github.com/limkvn/shipping-nowcast`
- **Live URL**: `https://limkvn.github.io/shipping-nowcast/`
- **Build script**: `scripts/build_nowcast_dashboard.py` generates a fully self-contained HTML file (all CSS, JS, and data are inlined — no external dependencies except Leaflet for maps)

The dashboard includes a client-side password gate (SHA-256 hash check), so the HTML is public on GitHub but the content requires a password to view.

## How Claude Deploys (Cowork Session Details)

Claude runs in a sandboxed VM where the filesystem resets between sessions. The deploy repo doesn't persist locally, so each session that needs to push follows this pattern:

```bash
# 1. Clone the deploy repo to a temp directory
git clone https://github.com/limkvn/shipping-nowcast.git /tmp/deploy_nowcast
cd /tmp/deploy_nowcast
git config user.email "limkvn@gmail.com"
git config user.name "Kevin Lim"

# 2. Build the dashboard
cd /path/to/Forecasting
python scripts/build_nowcast_dashboard.py

# 3. Copy, commit, push
cp outputs/nowcast/hormuz_nowcast_dashboard.html /tmp/deploy_nowcast/index.html
cd /tmp/deploy_nowcast
git add index.html
git commit -m "Update dashboard"
git push origin main
```

The re-clone is necessary every session because `/tmp` doesn't survive between sessions. Git credentials are handled via a token that the session has access to. The `git config` for user name/email also needs to be set each time since the VM starts fresh.

If you want Claude to push to a different repo for another project, just provide the repo URL and Claude can follow the same pattern: clone to `/tmp`, copy the generated file as `index.html`, commit, and push.

## Tips for Another Project

**Self-contained HTML is key.** GitHub Pages serves static files — there's no server-side processing. If your dashboard needs data, either inline it in the HTML or fetch it from a separate API. Our dashboard inlines all data as JS objects, making the file large (~2-3 MB) but completely standalone.

**Password protection is client-side only.** The HTML source is visible to anyone who views the repo or page source. The password gate deters casual viewing but is not true security. For sensitive data, consider a private repo with GitHub Pages disabled and a different hosting approach.

**Custom domain (optional).** Under Settings > Pages, you can configure a custom domain. Add a `CNAME` file to the repo root with your domain name.

**No CI/CD needed for simple cases.** For a single file, git push is the entire deployment. If you want automation (e.g., rebuild on a schedule), you can add a GitHub Actions workflow, but it's not necessary for manual updates.

**Cache busting.** GitHub Pages uses CDN caching. If viewers see stale content after a push, they may need to hard-refresh (Ctrl+Shift+R). You can add a query parameter to the URL (`?v=2`) to force a fresh load when sharing updated links.

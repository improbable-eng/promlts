# How to contribute to Docs/Website

`./docs` directory is used as markdown source files using [blackfriday](https://github.com/russross/blackfriday) to render Thanos website resources.

However the aim for those is to also have those `*.md` files renderable and usable (including links) via GitHub.

To make that happen we use following rules and helpers that are listed here.

## Front Matter

[Front Matter](https://gohugo.io/content-management/front-matter/) is essential on top of every markdown file if you want to link this file into any menu/submenu option. We use YAML formatting. This will render in GitHub as markdown just fine:

```md

---
title: <title>
type: ...
weight: <weight>
menu: <where to link files in>  # This also is referred in permalinks.
---
```

## Links

Aim is to match linking behavior in website being THE SAME as Github. This means:

* For files in Hugo <content> dir (so `./docs`). Put `slug: /<filename with extension>`
* For any sub dir add to `website/hugo.yaml` new dir as key `permalinks:` with `<dir>: /<dir>/:filename.md`

Then everywhere use native markdown absolute path to the project repository if you want to link to exact commit e.g:

```
[title](/Makefile)
```

Small [post processing script](../../scripts/website/websitepreprocess.sh) adjusts link for Hugo rendering.

NOTE: Spaces matters so: `[xxx]( link` and `[xxx] (link` will not work.

Why?

* Links works on GitHub
* Links works on website
* Markdown plugins works as expected (e.g IDE integrations)
* We use markdown-link-check to test links

## Sections/Menu

New menus `.Site.Menus` are added as soon as some file has Front Matter with certain `menu`.

Keep `menu` the same as sub-directory the file is in. This will help to manage all docs.

Show new menu section in main page by changing `website/layouts/_default/baseof.html` file.

## Adopters Logos

We'd love to showcase your company's logo on our main page and README! Requirements for the company:

* it is using Thanos on production
* it is a legal registered company
* it is happy to announce that you use Thanos publicly

If all those are met, add yourself in [`website/data/adopters.yml`](../../website/data/adopters.yml) like so:

```yml
- name: My Awesome Company
  url: https://wwww.company.com
  logo: company.png
```

Copy your company's logo in [`website/static/logos`](../../website/static/logos), make sure it follows these rules:

* Rectangle shape
* Greyscale is preferred but color is fine
* Keep it under 50KB

and create PR against Thanos `main` branch.

## White noise

We want all docs to not have any white noise. To achieve it, we provide [cleanup-white-noise.sh](https://github.com/thanos-io/thanos/blob/main/scripts/cleanup-white-noise.sh) under `scripts` to check. You can call it before a pull request, also PR test would call it too.

## Testing

### PR testing

On every PR we build the website and on success display the link to the preview under the checks at the bottom of the github PR.

### Local testing

To test the changes to the docs locally just start serving the website by running the following command and you will be able to access the website on `localhost:1313` by default:

```bash
make web-serve
```

## Deployment

We use [Netlify](https://www.netlify.com/) for hosting. We are using Open Source license (PRO). Thanks Netlify for this!

On every commit to `main` netlify runs CI that invokes `make web` (defined in [netlify.toml](../../netlify.toml))

NOTE: Check for status badge in README for build status on the page.

If main build for netlify succeed, the new content is published automatically.

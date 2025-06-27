FROM public.ecr.aws/lambda/python:3.9 AS stage

RUN yum install -y -q sudo unzip
ENV CHROMIUM_VERSION=1002910

# Install Chromium
COPY install-browser.sh /tmp/
RUN /usr/bin/bash /tmp/install-browser.sh

FROM public.ecr.aws/lambda/python:3.9 AS base

RUN yum install -y \
    acl adwaita-cursor-theme adwaita-icon-theme alsa-lib at-spi2-atk at-spi2-core \
    atk avahi-libs cairo cairo-gobject colord-libs cryptsetup-libs cups-libs dbus \
    dbus-libs dconf desktop-file-utils device-mapper device-mapper-libs elfutils-default-yama-scope \
    elfutils-libs emacs-filesystem fribidi gdk-pixbuf2 glib-networking gnutls graphite2 \
    gsettings-desktop-schemas gtk-update-icon-cache gtk3 harfbuzz hicolor-icon-theme hwdata jasper-libs \
    jbigkit-libs json-glib kmod kmod-libs lcms2 libX11 libX11-common libXau libXcomposite libXcursor libXdamage \
    libXext libXfixes libXft libXi libXinerama libXrandr libXrender libXtst libXxf86vm libdrm libepoxy \
    liberation-fonts liberation-fonts-common liberation-mono-fonts liberation-narrow-fonts liberation-sans-fonts \
    liberation-serif-fonts libfdisk libglvnd libglvnd-egl libglvnd-glx libgusb libidn libjpeg-turbo libmodman \
    libpciaccess libproxy libsemanage libsmartcols libsoup libthai libtiff libusbx libutempter libwayland-client \
    libwayland-cursor libwayland-egl libwayland-server libxcb libxkbcommon libxshmfence lz4 mesa-libEGL mesa-libGL \
    mesa-libgbm mesa-libglapi nettle pango pixman qrencode-libs rest shadow-utils systemd systemd-libs trousers ustr \
    util-linux vulkan vulkan-filesystem wget which xdg-utils xkeyboard-config \
    libyaml-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install Python dependencies for function
COPY requirements.txt /tmp/
RUN python3 -m pip install --upgrade pip -q
RUN python3 -m pip install -r /tmp/requirements.txt -q

COPY --from=stage /opt/chrome /opt/chrome
COPY --from=stage /opt/chromedriver /opt/chromedriver

# copy main.py
COPY main.py /var/task/
COPY selectors.yml /var/task/selectors.yml

WORKDIR /var/task

CMD [ "main.handler" ]

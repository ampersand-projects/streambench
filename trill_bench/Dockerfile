FROM ubuntu:18.04

ADD ./. /root/trill_bench
WORKDIR /root/trill_bench

RUN apt-get update
RUN apt-get install -y wget

ENV DEBIAN_FRONTEND=noninteractive

RUN wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb
RUN dpkg -i packages-microsoft-prod.deb

RUN apt-get update
RUN apt-get install -y apt-transport-https dotnet-sdk-3.1

RUN mv /usr/share/dotnet/sdk/$(dotnet --version)/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFx.props /usr/share/dotnet/sdk/$(dotnet --version)/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFX.props
RUN mv /usr/share/dotnet/sdk/$(dotnet --version)/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFx.targets /usr/share/dotnet/sdk/$(dotnet --version)/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFX.targets

RUN apt install -y gnupg ca-certificates
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
RUN echo "deb https://download.mono-project.com/repo/ubuntu stable-bionic main" | tee /etc/apt/sources.list.d/mono-official-stable.list
RUN apt update

RUN apt install -y mono-devel

RUN dotnet build -c Release

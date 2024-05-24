<p align="center">
  <a href="https://getwren.ai">
    <picture>
      <source media="(prefers-color-scheme: light)" srcset="./misc/wrenai_logo.png">
      <img src="./misc/wrenai_logo.png">
    </picture>
    <h1 align="center">Wren Engine</h1>
  </a>
</p>

<p align="center">
  <a aria-label="Canner" href="https://cannerdata.com/">
    <img src="https://img.shields.io/badge/%F0%9F%A7%A1-Made%20by%20Canner-blue?style=for-the-badge">
  </a>
  <a aria-label="License" href="https://github.com/Canner/wren-engine/blob/main/LICENSE">
    <img alt="" src="https://img.shields.io/github/license/canner/wren-engine?color=blue&style=for-the-badge">
  </a>
  <a aria-label="Join the community on GitHub" href="https://discord.gg/5DvshJqG8Z">
    <img alt="" src="https://img.shields.io/badge/-JOIN%20THE%20COMMUNITY-blue?style=for-the-badge&logo=discord&logoColor=white&labelColor=grey&logoWidth=20">
  </a>
  <a aria-label="Follow us" href="https://x.com/getwrenai">
    <img alt="" src="https://img.shields.io/badge/-@getwrenai-blue?style=for-the-badge&logo=x&logoColor=white&labelColor=gray&logoWidth=20">
  </a>
</p>

> Wren Engine is the semantic engine for LLMs, the backbone of the semantic layer. 

Useful links
- [WrenAI Website](https://getwren.ai)
- [Wren Engine Documentation](https://docs.getwren.ai/engine/get_started/what_is)

## 🎯 Our Mission

Wren Engine is designed as a standalone semantic engine, which you can easily implement with any AI agents, you can use it as a general semantic engine for the semantic layer.

<img src="./misc/wren_engine_flow.png">

## 🤔 Concepts

- [Introducing Wren Engine](https://docs.getwren.ai/engine/get_started/what_is)
- [What is semantics?](https://docs.getwren.ai/engine/concept/what_is_semantics)
- [What is Modeling Definition Language (MDL)?](https://docs.getwren.ai/engine/concept/what_is_mdl)
- [Benefits of Wren Engine with LLMs](https://docs.getwren.ai/engine/concept/benefits_llm)

## 🚧 Project Status
Wren Engine is currently in the alpha version. The project team is actively working on progress and aiming to release new versions at least biweekly.

## ⭐️ Community

- Welcome to our [Discord server](https://discord.gg/5DvshJqG8Z) to give us feedback!
- If there is any issues, please visit [Github Issues](https://github.com/Canner/wren-engine/issues).

## 🚀 Get Started

Check out our latest documentation to get a [Quick start](https://docs.getwren.ai/engine/get_started/quickstart).

## 🙌 How to build?

### Normal Build

```bash
mvn clean install -DskipTests
```

### Build an executable jar

```bash
mvn clean package -DskipTests -P exec-jar
```

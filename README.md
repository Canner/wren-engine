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

👉 Check out our website: https://getwren.ai

## 🎯 Our Mission

Wren Engine's mission is to provide the semantic engine for LLMs, serving as the backbone of the semantic layer and delivering business context to BI and LLMs.

## 🤔 Benefits using Wren Engine

The semantic layer serves as an intermediary, translating complex data structures into a business-friendly format, enabling end-users to interact with data using familiar terminology without needing to understand the underlying technical complexities. It simplifies the user experience by mapping business terms to data sources, defining relationships, and incorporating predefined calculations and aggregations.

On the other hand, the ***semantic engine - Wren Engine***, operates behind the scenes, powering the semantic layer with advanced capabilities to design, interpret, and manage the **modeling definition language**. This engine is responsible for the intricate processing that defines and maps metadata, schema, terminology, data relationships, and the logic behind calculations and aggregations through an analytics-as-code design approach, in which developers can define how the semantic layer operates in a structured way.  By leveraging a semantic engine, organizations can ensure that their semantic layer is developer-friendly that is intelligently designed to reflect the nuanced relationships and dynamics within their data.

The semantic engine enables more sophisticated handling of data semantics, ensuring that the semantic layer can effectively meet users' diverse needs, facilitating more informed decision-making and strategic insights.

## 🚧 Project Status
Wren Engine is currently in the alpha version. The project team is actively working on progress and aiming to release new versions at least biweekly.

Full documentation is also working in progress and will soon be released.

## ⭐️ Community

- Welcome to our [Discord server](https://discord.gg/5DvshJqG8Z) to give us feedback!
- If there is any issues, please visit [Github Issues](https://github.com/Canner/wren-engine/issues).


## 🙌 How to build?

### Normal Build

```bash
mvn clean install -DskipTests
```

### Build an executable jar

```bash
mvn clean package -DskipTests -P exec-jar
```

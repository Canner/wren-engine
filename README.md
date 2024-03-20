<p align="center">
  <img src="https://imgur.com/xUamgKl.png" width="1000" >
</p>

<p align="center">
  <a aria-label="Canner" href="https://cannerdata.com/">
    <img src="https://img.shields.io/badge/%F0%9F%A7%A1-Made%20by%20Canner-orange?style=for-the-badge">
  </a>
  <a aria-label="License" href="https://github.com/Canner/wren/blob/main/LICENSE">
    <img alt="" src="https://img.shields.io/github/license/canner/wren?color=orange&style=for-the-badge">
  </a>
  <a aria-label="Join the community on GitHub" href="https://discord.gg/ztDz8DCmG4">
    <img alt="" src="https://img.shields.io/badge/-JOIN%20THE%20COMMUNITY-orange?style=for-the-badge&logo=discord&logoColor=white&labelColor=grey&logoWidth=20">
  </a>
  <a aria-label="Follow us" href="https://twitter.com/getwren">
    <img alt="" src="https://img.shields.io/badge/-@getwren-orange?style=for-the-badge&logo=twitter&logoColor=white&labelColor=gray&logoWidth=20">
  </a>
</p>

## What is Wren ?

Define relationships, metrics, and expressions consistently with [Wren](https://www.getwren.ai/). Experience a unified data view and let us generate on-demand SQL for a
composable, reusable approach.

Wren unites your data in one expansive view, putting an end to scattered metrics and inconsistent queries. We enable the definition of relationships and easy computation of
metrics. By generating SQL on-demand, we ensure a consistent, composable, and reusable approach, making data analytics smoother and more efficient.

![overview of Wren](https://imgur.com/o8sdYRC.png)

## Examples

Need Inspiration?! Discover a [selected compilation of examples](https://www.getwren.ai/docs/example/tpch) showcasing the use of Wren!

## Installation

Please visit [the installation guide](https://www.getwren.ai/docs/get-started/installation).

## How Wren works?

💻 **Modeling**

Wren introduces a powerful Model Definition Language (MDL) enabling you to shape your data effortlessly. Transform your tables into `Models` and establish connections
using `Relations`. Unlock the true potential of Business Intelligence (BI) by formulating impactful `Metrics` with Wren's intuitive MDL.

🚀 **Access**

Wren offers a join-free SQL solution that allows users to query models as if accessing one expensive view. Focuses on enabling users to effortlessly perform BI-style queries.
Traditional SQL users can easily write readable and semantically rich SQL queries that are also easy to maintain.

🔥 **Deliver**

Wren implements the PostgreSQL Wire Protocol for easy integration, enabling users to effortlessly deliver data to existing systems and various tools, such as BI tools and Database
IDEs.

## Documentation

More details, please see our [documentation](https://www.getwren.ai/docs/get-started/intro)!

## Community

- Welcome to our [Discord server](https://discord.gg/ztDz8DCmG4) to give us feedback!
- If there is any issues, please visit [Github Issues](https://github.com/Canner/wren/issues).

## Special Thanks

- [Trino](https://github.com/trinodb/trino)

  Wren's SQL analysis layer is fundamentally based on a modified version of Trino's SQL analysis engine. Trino's clean, readable, and maintainable SQL analysis layer served as a
  significant inspiration, allowing us to focus more on researching and exploring Wren's syntax and models.
- [CrateDB](https://github.com/crate/crate)

  Wren's PostgreSQL Wire Protocol is inspired by CrateDB. CrateDB's implementation of the PostgreSQL Wire Protocol is very clear and easy to maintain, which significantly helped
  us clarify the focus and architecture during the early development stages. This allowed us to concentrate on researching and exploring Wren's syntax and models.
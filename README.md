<p align="center">
  <img src="https://imgur.com/xUamgKl.png" width="600" >
</p>

<p align="center">
  <a aria-label="Canner" href="https://cannerdata.com/">
    <img src="https://img.shields.io/badge/%F0%9F%A7%A1-Made%20by%20Canner-orange?style=for-the-badge">
  </a>
  <a aria-label="License" href="https://github.com/Canner/vulcan-sql/blob/develop/LICENSE">
    <img alt="" src="https://img.shields.io/github/license/canner/vulcan-sql?color=orange&style=for-the-badge">
  </a>
  <a aria-label="Join the community on GitHub" href="https://discord.gg/ztDz8DCmG4">
    <img alt="" src="https://img.shields.io/badge/-JOIN%20THE%20COMMUNITY-orange?style=for-the-badge&logo=discord&logoColor=white&labelColor=grey&logoWidth=20">
  </a>
</p>

## What is Accio

Define relationships, metrics, and expressions consistently with [Accio](https://www.getaccio.ai/). Experience a unified data view and let us generate on-demand SQL for a
composable, reusable approach.

Accio unites your data in one expansive view, putting an end to scattered metrics and inconsistent queries. We enable the definition of relationships and easy computation of
metrics. By generating SQL on-demand, we ensure a consistent, composable, and reusable approach, making data analytics smoother and more efficient.

![overview of Accio](https://imgur.com/o8sdYRC.png)

## Examples

Need Inspiration?! Discover a [selected compilation of examples](https://www.getaccio.ai/docs/example/tpch) showcasing the use of Accio!

## Installation

Please visit [the installation guide](https://www.getaccio.ai/docs/get-started/installation).

## How Accio works?

💻 **Modeling**

Accio introduces a powerful Model Definition Language (MDL) enabling you to shape your data effortlessly. Transform your tables into `Models` and establish connections
using `Relations`. Unlock the true potential of Business Intelligence (BI) by formulating impactful `Metrics` with Accio's intuitive MDL.

🚀 **Access**

Accio offers a join-free SQL solution that allows users to query models as if accessing one expensive view. Focuses on enabling users to effortlessly perform BI-style queries.
Traditional SQL users can easily write readable and semantically rich SQL queries that are also easy to maintain.

🔥 **Deliver**

Accio implements the PostgreSQL Wire Protocol for easy integration, enabling users to effortlessly deliver data to existing systems and various tools, such as BI tools and Database
IDEs.

## Documentation

More details, please see our [documentation](https://www.getaccio.ai/docs/get-started/intro)!

## Community

- Welcome to our [Discord server](https://discord.gg/ztDz8DCmG4) to give us feedback!
- If there is any issues, please visit [Github Issues](https://github.com/Canner/accio/issues).

## Special Thanks

- [Trino](https://github.com/trinodb/trino)

  Accio's SQL analysis layer is fundamentally based on a modified version of Trino's SQL analysis engine. Trino's clean, readable, and maintainable SQL analysis layer served as a
  significant inspiration, allowing us to focus more on researching and exploring Accio's syntax and models.
- [CrateDB](https://github.com/crate/crate)

  Accio's PostgreSQL Wire Protocol is inspired by CrateDB. CrateDB's implementation of the PostgreSQL Wire Protocol is very clear and easy to maintain, which significantly helped
  us clarify the focus and architecture during the early development stages. This allowed us to concentrate on researching and exploring Accio's syntax and models.
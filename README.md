# elevations-service
An elevations service for development during the WindEurope Annual Conference in Copenhagen, 2023

In the abstract "Real time in real time: from science to data service at WindEurope" we set out a challenge. During the conference, Octue engineers, assisted by engineers from partner organisations and other contributors, will create **from scratch** a data service, accessible to anybody in the industry, delivering earth surface elevations data along with their scientific provenance, at a resolution fit for wind farm design and resource assessment purposes.

We will make use of publicly available raw data from NASA and ESA datasets, so will not generate our own data. The challenge will be to undertake the data engineering required to make accessing subsets of those datasets frictionless.

> The IEA Task 43 data maturity roadmap points toward a single goal, “Frictionless data access within and between organizations by 2030”

### The challenge

- Get elevation data
- In a consistent and clearly understandable form
- Including scientific provenance
- Including resolution
- For a given location/area
- With minimal expertise in scripting or computer science
- With no data cleaning
- With no manual intervention
- With clear error handling and management when things go wrong
- With speed sufficient for use in real-time (or at least on-demand) data processing

### The whole point is to teach - here's what you'll learn

As a delegate, you can follow on throughout the challenge, by clicking the 'watch' button ([top right on the github page](https://github.com/octue/elevations-service)).

You can follow us on e'll do various sessions throughout WindEurope and be available in the hall for you to ask questions. At each stage, you'll be able to take a deep look in, and follow along with us, as we explain how and why we do things:

- System architecture
  - Where we get data
  - When we get it (at what times and why)
  - How we'll store it and why we do it that way
  - How we'll access it (thinking about speed and caching... can we get extra performance for free?)
  - Security and cost considerations
  - How we'll manage disparate data sources with varying resolution

- Git, GitHub and coding best practices
  - Style guides (why and what)
  - Code quality and consistency (introducing pre-commit checks)
  - Conventional commits (improve clarity, get release notes and versioning for free)
  - Test Driven Development and why it helps
  - Release flow and what Pull Requests are for

- DevOps
  - Continuous Integration, how to do it with GitHub Actions and why it's helpful
  - How to manage infrastructure (using terraform, if we have time!)
  - Building a data lake
  - Preventing costs from escalating
 
- Data engineering and data services
  - Writing a schema to describe the data you deliver
  - Wrapping scientific code up as a data service
  - Calling the data service as an end user
  
  




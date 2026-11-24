I'm# Web-Scrapping
Web Scrapping 
Implementing Content Similarity on Web Scrapped Data

Here I have used BeautifulSoup for scrapping the data from the given Url
After scrapping on different Url's saved the each text data on 3 different txt files. 

# Content Similarity
Similarity between two sentences can be achieved by calculating the Cosine Similairty or Euclidian Distance between each word vector
Here I have used Cosine Similarity to calculate the similarity between the words
Initially I tought of using 2 different files for comparing each and every sentence from 2 files.
But given the time constraint, the similarity is done only using one text file
 
cosine similarity is the dot/scalar product of two vectors divided by the product of their Euclidian norms
Process to do the Similiarity
1. Raw text data is preprocessed by stemming, lemmatisation, tokenization
2. Term frequency of each word is calculated and a dictionary of the word frequency is created
3. The numbers are used to create a vector for each document where each component in the vector stands for the term frequency in that document. Let n be the number of documents and m be the number of unique terms. Then we have an n by m tf matrix.
4. The core of the rest is to obtain a “term frequency-inverse document frequency” (tf-idf) matrix. Inverse document frequency is an adjustment to term frequency. 
5. tf-idf scales up the importance of rarer terms and scales down the importance of more frequent terms relative to the whole corpus.
                   
                   tf-idf = tf * idf
                   idf(t) = log(n+1/df(d,t)+1)+1
                   where n is the total number of documents and df(d, t) is the number of documents in which term t appears
                   as df(d, t) gets smaller, idf(t) gets larger
6. The calculated tf-idf is normalized by the Cosine Similarity formula so that each row vector has a length of 1
7. The normalized tf-idf matrix should be in the shape of n by m. A cosine similarity matrix (n by n) can be obtained by multiplying the if-idf matrix by its transpose (m by n).

I just did similarity to just one document, due to time constraint, full model development is not done


You are a senior software architect joining this project. Your job is to write the project's single source of truth: documentation detailed enough that a new engineer can understand, run, change, and support the system without asking anyone.

## Ground rules
- Read the actual code, configs, IaC, pipelines, tests, and existing docs before writing. Do not guess.
- Every important claim must cite where it comes from (file path, and line or symbol when useful), e.g. `src/api/handler.py:42`.
- If something can't be found in the repo, write "UNKNOWN – needs confirmation" and add it to the open-questions list. Never make up behavior, owners, URLs, or numbers.
- Never copy secrets, keys, tokens, or account IDs into the docs. Name the variable or secret instead.
- Use Mermaid for diagrams. Use tables for inventories.
- Write plainly for an engineer who has never seen this repo.

## Process
1. Scan the full repo tree first. List every top-level folder and what it seems to do.
2. Find entry points, build files, dependency manifests, IaC (Terraform/CloudFormation/CDK), CI/CD workflows, Dockerfiles, env/config files, and tests.
3. Trace the main request and data flows from start to finish.
4. Write the docs below into a `/docs` folder, one file per section, plus a `/docs/README.md` index that links to all of them.
5. Finish by checking each doc against the code and fixing anything that doesn't match.

## Docs to write
00-overview.md: what the project is, the problem it solves, who uses it, main features, and a glossary of domain and internal terms.
01-architecture.md: high-level architecture diagram, components and what each one owns, how they talk to each other (sync or async, protocols), external dependencies and integrations, and key design decisions with the reasons for them where the code or comments show them.
02-repo-structure.md: annotated folder tree, what lives where, naming conventions, and where to put new code.
03-code-walkthrough.md: entry points, core modules and classes and functions, main execution paths explained step by step, plus sequence diagrams for the top 3–5 flows.
04-data.md: data models and schemas, databases and storage, data flow and lineage, retention, and any PII or sensitive data handling.
05-apis-and-interfaces.md: every endpoint, event, queue, topic, CLI command, or public function, with inputs, outputs, auth, and error behavior.
06-infrastructure.md: cloud resources by environment, IaC modules and how they're wired, networking, IAM roles and permissions, cross-account setup, and an infra diagram.
07-configuration.md: every environment variable and config key, with purpose, default, required or optional, and where it's read. Also secrets management.
08-local-setup.md: prerequisites, exact commands to install, build, run, and test locally, and common setup problems.
09-ci-cd-and-deployment.md: pipelines step by step, environments, how to promote a release, rollback, approvals, and gates such as policy checks.
10-testing.md: test types, how to run them, coverage gaps, and how to add a test.
11-security-and-compliance.md: authentication and authorization, guardrails, logging of sensitive actions, dependency risks, and policy-as-code.
12-observability-and-operations.md: logging, metrics, alerts, dashboards, runbooks for likely failures, and troubleshooting.
13-dependencies.md: a table of libraries, versions, and what each is used for. Flag outdated or risky ones.
14-known-issues-and-tech-debt.md: TODOs and FIXMEs found in code, fragile areas, dead code, inconsistencies, and suggested improvements ranked by impact.
15-open-questions.md: everything marked UNKNOWN, grouped by who would likely know the answer (dev, infra, security, product).
16-onboarding-path.md: a reading order for a new engineer, covering day 1, week 1, and the first safe change to make.

Start with step 1 and show me the repo map and your doc plan before writing the full files.

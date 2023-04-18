# Thought Process

- I started by reading the index file into chunks and analyzing the first a few chunks.
- With the help of the schema reference, I got a basic understanding of the data.
- Then I spent some time figuring out how to parse the data into json objects between chunks, as each chunk is not a valid json object by itself.
- Then I started to look for the keywords 'PPO' and 'New York' in the parsed json objects.
- I soon realized the plan name never had 'New York', but might have 'PPO' and the descriptions of in-network files might have 'New York' and 'PPO'. So I used this information to filter the reporting plans.
- Initially I thought I needed to get all reporting plans that meet the above conditions, but soon realized processing the index file just took forever and there were too many reporting plans that met the conditions. So I decided to sample a few reporting plans by looking up the EIN (plan id) on Anthem's EIN lookup system and found the files are mostly identical.
- I sampled a few more reporting plans that met the conditions, and got the same result. They all have the same `NY_PPO` files.
- With close to zero knowledge of the US health system, my guess is those files are probably what I was looking for. My speculation is that the PPO network that covers any plan is essentially all the PPO network. So I just need to find one matching plan and use it to query the EIN lookup system.
- In a real world environment, I will start consulting colleagues, clients, and health system experts for confirmation, but for this exercise I just carried on with the assumption.
- The code should be straightforward. I have added comments to explain the decisions made along the way.

# Setup

`requests` is the only non built-in python library used, so I didn't bother creating a requirements file. Just run

``` bash
pip3 install requests
```

The python version I have is `3.9.6`

Run

``` bash
python3 script.py
```

# Result

The result is in `urls.json`.

# !!! End of my addition

# Serif Health Takehome Interview

This repository contains the files and instructions for our takehome engineering interview. Please *locally* copy to your own public repo or [import](https://github.com/new/import) to your github account for use in sharing solutions back to us. Direct public forks and pull requests will expose your identity and solution to other candidates also working on this interview question, and we want the interview process to be fair for everyone.

## Context

Serif Health was founded with a mission to make the US healthcare system more transparent, efficient, and affordable for everyone. One of the challenging problems we're solving at Serif Health is making healthcare *pricing* data transparent and uniform for all market participants. There are myriad reasons this is difficult.

At the macro level:

- Most data in healthcare is protected by law, sensitive by default and tends to be locked up in proprietary systems or data formats.
- The data aggregators and clearinghouses that do have access to clean, normalized bulk data assets tend to employ extremely expensive and restrictive licensing terms.
- While recent price transparency laws have required hospitals and payers to publish their pricing, compliance and data sharing occurs at varying levels of completeness and consistency.  

At the micro level:

- Medical billing and coding for a specific procedure can be very complicated and is contingent on place of service, patient history and comorbidities, structure of insurance arrangements, so on and so forth. Many procedures are lots of N of 1 type cases.
- Insurance companies (payers) establish pre-negotiated non-published contracted rates with each facility, physician group, or health system that reimburses the healthcare provider at a rate and structure very different from what is 'charged'.

Summed together, all this complexity contributes to a general lack of transparency and market efficency in our healthcare system.

## Objective

In July 2022, payers published their pricing data under the Transparency in Coverage Act. Those published files are extremely large and require some forethought and skill to be able to work with them - your task is to write code that can open an index file, stream through it, and isolate a set of network files in the index.

Our customers typically want to know and compare reimbursement rates for healthcare services from specific payers. E.g., what does UnitedHealthcare reimburse orthopedic surgeons in Texas for total knee replacement surgery? To get there, we need to go to the payer's website, look up their MRF file list in a table of contents or index file, pull the MRF, extract the data, and we have our answer.

For this interview, we'll give you the web URLs and we'll skip MRF processing for now. We'd simply like to know, *what is the list of file URLs that represent the Anthem PPO network in New York state*?

## Inputs

The input to this takehome are the Anthem machine readable index file [table of contents](https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2023-04-01_anthem_index.json.gz), and Anthem's EIN lookup service, located here [Anthem EIN lookup](https://www.anthem.com/machine-readable-file/search/).

You should write code that can open the machine readable index file and process it according to the schema published at [CMS' transparency in coverage repository](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/table-of-contents), so you can extract the data requested below.

## Outputs

Your output should be the list of URLs corresponding to Anthem's New York State PPO network. Make sure to read through the hints and pointers section before declaring your solution complete.

## Hints and Pointers

As you start working with the index, you'll quickly notice that the index file itself is extremly large, data is very frequently repeated, plan information seems to be named after small businesses in various regions around the country, and that there's lots of different url styles. Think about this - how do you handle the file size and format efficiently? What could the url and filename format mean for identifying network class (PPO vs. HMO) and boundaries (NY vs. NJ)? How do the Plan IDs in the file intersect with the Anthem EIN lookup? How does the EIN lookup help you figure out the PPO?

Use your best judgement to proceed here, and discuss your decisions in your writeup.

### Deliverable

You should [send us](mailto:engineering@serifhealth.com) a link to a public repository or zip file that contains at miminum:

1. The script or code used to parse the file and produce output.
2. The setup or packaging file(s) required to bootstrap and execute your solution code
3. The output URL list.
4. A README file, explaining your solution, how long it took you to write, how long it took to run, and the tradeoffs you made along the way.

## Expectations

### Time vs Quality

We are a small engineering team with limited resources, and often have to make hard tradeoffs to meet deadlines and make rapid forward progress. We do not want this takehome to take more than a few hours out of your day. So, please timebox coding your solution to two hours max, and know that you have the opportunity to discuss the tradeoffs you made when submitting your solution. Experienced engineers should be able to complete the coding portion in about 90 minutes, perhaps less if you have prior healthcare experience. If you think this will take you dramatically more time than that, let us know before starting the takehome so we can discuss why.

If you finish early, we'd recommend adding additional notes or commentary to the README (e.g. discussion of performance characteristics, how you would ideally test/deploy/run your code in a production environment, feature iterations that might come next, so on), but please don't exceed the timebox doing so.

### Language Choice

You can choose any language you want, but your solution should be portable enough to run on someone else's machine.

### Dependencies

You can and *probably should* use dependencies (JSON parsers, type validators, etc) and libraries from public package managers in your language of choice. Again, your solution should be portable enough to run on someone else's machine, so if you leverage packaged dependencies this please make sure relevant setup instructions to install the dependencies and execute the solution are included.

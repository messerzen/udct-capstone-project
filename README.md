#### Step 1: Scope the Project and Gather Data

Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:

- Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.

- Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

## Dataset information

For this project the dataset of **Federal Revenue of *Brazil* (RFB)** will be used. The dataset contains public information about Brazilian companies such industry activity description ([CNAE](https://pt.wikipedia.org/wiki/Classifica%C3%A7%C3%A3o_Nacional_de_Atividades_Econ%C3%B4micas)), address, contacts information, business size and company age. The fully dataset contains about ten tables distributed main tables and  dimension tables. The three main tables are:

- **Establishments:** contains information about [CNPJ](https://en.wikipedia.org/wiki/CNPJ) (National Register of Legal Entities). The *CNPJ* consists of a 14-digit number formatted as  XX.XXX.XXX/0001-XX — **The first eight digits identify the company**, the four digits after the  slash identify the branch or subsidiary ("0001" defaults to the  headquarters), and the last two are check digits. The *CNPJ* must be informed on any invoice of any company, as well as on the packaging of any industrialized product. In this dataset each establishments of an specific company will by represented by one record.
- **Company** - Contains information about a company, such business size and [legal nature](https://pt.wikipedia.org/wiki/Natureza_jur%C3%ADdica). 
- **Partners**: - Contains information about company partners and their respective roles.

The others tables are dimension tables, that describes information contained in the main three tables. They are:

- CNAE -  identifies the establishment activity 
- Country.
- Cities. 
- Legal nature description.
- Status
- Motive for the current status (if applicable).
- Roles description (for the company partners).

### Dataset size

The dataset contains about 54 million rows (based on the establishments tables). For the company tables, the size is around 40 million rows. This difference occurs because one company can have multiples establishments in the same registration number (The first eight digits identify of the CNPJ).

### Dataset updating

Based on the information provided by the **Federal Revenue of *Brazil* (RFB)**, this dataset is updated once a month.

## Use cases

For this project, the dataset will be prepared considering two scenarios:

### Scenario 01

A company wants to expand its market share over the country and needs to identify others companies with similar characteristics of its current clients. The profile is based on two main characteristics: business size, establishments age and activity (CNAE).

### Scenario 02

The same company wants to reproduce a dashboard to monitoring if a specify business activity is growing or decreasing in specifics states. 

Base on these two scenarios a database will be implemented in Amazon Redshift, containing the main information about the establishments and companies. And the same database will be used to create aggregated tables with the information that will be used in the dashboards.
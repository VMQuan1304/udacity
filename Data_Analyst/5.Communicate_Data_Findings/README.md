# (Dataset Exploration Title)
## by Vu Minh Quan


## Dataset

This is Health insurance dataset containing 1338 rows and 7 variables including age of responsers, sex, bmi (body mass index), children, smoker, region and charges. After checking step, the dataset has no null values and outliers. To deep dive each variables, please follow the explaination below:

age: age of primary beneficiary
sex: insurance contractor gender, female, male
bmi: Body mass index, providing an understanding of body, weights that are relatively high or low relative to height, objective index of body weight (kg / m ^ 2) using the ratio of height to weight, ideally 18.5 to 24.9
children: Number of children covered by health insurance / Number of dependents
smoker: Smoking
region: the beneficiary's residential area in the US, northeast, southeast, southwest, northwest.
charges: Individual medical costs billed by health insurance


## Summary of Findings

* The chart shows the percentage of male and female in dataset. It is clear that ratio of female responsers (49%) nearly equal to male ones (50%)
* The chart reprents the age of responsers are highly 20 years old and the range of age of responsers is 20-65
* The chart illustates the normal distribution of this variable. The range of this is from nearly 17 to over 50 point.
* Responsers with no children are the highest bar. That can be caused by the large 20 year-old responsers. The families with 4 or 5 children are the lowest bar. We can take deep dive in to this question in Bivariate Exploration part.
* The pie chart shows the percentage of smokers is fourtimes larger than ones who do not smoke. 
* There are 4 regions in dataset and percentage of responsers who living in regions is nearly the same as the others.

> After investigation, smokers, sex are variables that impact on charges variable. To be specifc, the responsers who do not smoke intend to invest more money on health insurance than smokers. Besides, the number of male who pay insurance fee more than male. Age, children, bmi variables do not affect on health insurance much.

> Due to above investigation, I found that age element does not impact on number of children. For example, 20 year-old responsers with 5 children occur highest part and over 50 responsers with no children have the large part, also. 


> The more bmi point the smokers have, the higher charges they get. Besides, responsers who have less children in insurance health program will have more medical fee paid by insurance health, especially smokers. Moreover, sex of smokers and non-smokers does not impact much on medical fee paid by insurance health. Finally, sex and smoking behavior do not affect on bmi, which is Body mass index, providing an understanding of body, weights.

> The more bmi point the smokers have, the higher charges they get. Besides, responsers who have less children in insurance health program will have more medical fee paid by insurance health, especially smokers. That can prove smokers with no children can have high medical fee paid by insurance health. Moreover, responsers have more children, the less smokers we have.

> In conclusion, the more bmi point the smokers have, the higher charges they get. Besides, responsers who have less children in insurance health program will have more medical fee paid by insurance health, especially smokers. Moreover, sex of smokers and non-smokers does not impact much on medical fee paid by insurance health. Finally, sex and smoking behavior do not affect on bmi, which is Body mass index, providing an understanding of body, weights. Moreover, responsers have more children, the less smokers we have. 

### Did you observe any interesting relationships between the other features (not the main feature(s) of interest)?

> In conclusion, responsers who have less children in insurance health program will have more medical fee paid by insurance health, especially smokers. Moreover, sex of smokers and non-smokers does not impact much on medical fee paid by insurance health. Sex and smoking behavior do not affect on bmi, which is Body mass index, providing an understanding of body, weights. Moreover, responsers have more children, the less smokers we have. 


## Key Insights for Presentation

* The more bmi point the smokers have, the higher charges they get
* The more bmi point the smokers have, the higher charges they get
* Responsers who have less children in insurance health program will have more medical fee paid by insurance health
# Data Analytics Task @ HTW 2020

Deadline 2020-01-28 / 23:59

## Tips
- Ausschließlich Open Source Software, die mit einer freien Lizenz veröffentlicht wird
- Keine proprietären Lösungen von Google, Amazon, Microsoft und weitere Unternehmen (BigQuery etc.)
- Ohne Abhängigkeiten zu poprietären Diensten.

Ergänzend noch einige Tooling-Empfehlungen für Entwicklung:

- Repository: Gitlab.com (Warum nicht Github? Github wird nicht unter einer freien Lizenz veröffentlicht). 
- Für Deployment: Docker, Docker-Compose, ggf. Ansible, Bash oder Python Scripte. Die Erstellung einer eigenen VM als Laufzeitumgebung möglichst vermeiden, würde aber akzeptiert werden - hierfür: Vagrant
- Als IDE: VSCode, Spyder, Jupyter Notebooks (Note: PyCharm wird nicht mit einer freien Lizenz veröffentlicht). 
- Softwarekomponenten für Analyse: Apache Spark, ggf. Python Pandas, scikit-learn.
- Programmiersprache: Python
- Solange ihre Toolauswahl im Sinne der Aufgabenstellung ist, können Sie natürlich auch andere Tools auswählen. Ich möchte Sie sogar grundsätzlich dazu motivieren, meine Empfehlungen zu hinterfragen und zu challengen :-)

## Task
Perform pattern recognition on a Dataset of ~15 Mio data points of Internet traffic retrieved by the firewall of Frachtwerk GmbH between Sept-Dec 2020.
− First, explore the data and describe it, e. g. used protocols, target ports, distribution and maximum bandwidth, point of time of high bandwidth...
− Then identify patterns. Focus especially on security related contexts, i. e. identify port scanners, geolocate ip addresses to find server locations that perform denial-of-service attacks, identify different types of attacks, recognise different attackers when they perform several attacks, find patterns especially in spikes, identify correlations with time intervals... Hint: Do not do all of this! Select carefully what you think is a sufficient but somehow complete analysis!
− Finally, present the most relevant results in an appropriate way, e. g. using statistics, KPIs and graphical charts.

### Data
- Data download: https://storage.frachtwerk.de/f/64468b1afb984eea9ff3/ Documentation of the data:
- Data is the log of all IP packets that are blocked due to firewall rules. These packets do not reach the internal network at Frachtwerk GmbH.
- The firewall is based on the pfsense, a popular Open Source product. Ist documentation can be used to understand the data: https://docs.netgate.com/pfsense/en/latest/monitoring/logs/raw-filter- format.html
- Hint: „gw01.frachtwerk“ is the name of the node at which the firewall si running. This can be ignored. The field „TOS“ (in documentation section IPv4) as well as some others (please see yourself) can be ignored, too.
Note: The data must not be distributed outside this project to third parties!

### Hints
− As methodology,applying the CRISP-DM might help you as a guidance. See the lecture #2 for more details.
− For geolocation, this tool might be interesting for you https://dev.maxmind.com/geoip/geoip2/geolite2/ https://dev.maxmind.com/geoip/geoip2/downloadable/#Max Mind_APIs

# Task Framework
## Overview
- Perform software development on the taskgivenlaterin this slide deck
- Summarize your findings and create appropriate documentions
- Present your findings in 15 Min. + answer questions on the topic (~5-10 Min after presentation)

## Conceptual Documentation
Conceptual documentation of the work you‘ve done. In the form of written text, in 4-7 pages (without table of content, bibliography and appendix).

In this part, please discuss especially:
- Which analysis tasks you selected and why you think your selection is appropriate and somehow complete
- How you selected the technology for your work
- Your implemented IT-Architecture
- Your results of the analysis
- Any other findings throughout the project that you‘d like to share Format: .pdf

## Source Code
− Content: Source code as export of a Git repository
− Format:.zip
− The technical documentation is to be put into the Git repository. There is no minimal length of the technical software documentation. Choose it‘s length according to the understanding of a good documentation later in this slide deck.
− The software must be able to run on a Debian or Ubuntu based GNU/Linux on top of hardware ressources as of a typical standard laptop.
− Docker containers are highly recommended to be used.

## Presentation
− Summarize your project and your results in a presentation.
− Format:.pdf
− Note: In the presentation session, there might be a jury of external professionals that are interested in your results.

## Technical expectations
− Apply best practices in software development, esp.:
− Use Git and standards for „good“ Git messages.
− Create a good technical documentation inside (inline) and outside of the source code (in README.MD or subfolder „docs“ if needed)
− Consequently follow the principle „Everything-as-code“
− Use „modern“ resp. cloud-native software technologies

## Expecation on documentation
- A good documentation enables a user with only basic IT skills to use the documented artefact (e. g. a piece of software) completely himself.
- The documentation should answer all questions that could come up by a user
- The documentation explains some technological concepts, if more than basic IT skills are required to understand how the software can be used
- The documentation explaines especially the use cases of setup requirements and dependencies, setup itself, and teardown.

## Other non-tech expecations
− Application of scientific standards. Example: every quotation shall be declared as such, regardless of it‘s document type (essay, documentation, source code...)
− Please follow this naming Scheme example for all files except source code files inside the Git repo export: 
-- 2021-05-08-Group-1-Log-Analysis-Conceptual-documentation
-- 2021-05-08-Group-1-Log-Analysis-Source
-- 2021-05-08-Group-1-Log-Analysis-Presentation
− Add a license to all pieces of your work and explain your decision in the conceptual documentation. You find a good overview of licenses here: https://en.wikipedia.org/wiki/Free-software_license
Hint: Publishing your work under a free software license is highly appreciated by todays‘ employers.

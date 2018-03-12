# Metadata providers for [Active Query Builder 3 for .NET](https://www.activequerybuilder.com/product_net.html) (any edition)

## What is Active Query Builder for .NET?
Active Query Builder is a component suite for different .NET environments: [WinForms](https://www.activequerybuilder.com/product_winforms.html), [WPF](https://www.activequerybuilder.com/product_wpf.html), [ASP.NET](https://www.activequerybuilder.com/product_asp.html). 
It includes:
- Visual SQL Query Builder,
- SQL Parser and Analyzer,
- API to build and modify SQL queries,
- SQL Text Editor with code completion and syntax highlighting.

##### Details:
- [Active Query Builder website](http://www.activequerybuilder.com/),
- [Active Query Builder for .NET details page](http://www.activequerybuilder.com/product_net.html).

## How do I get Active Query Builder?
- [Download the trial version](https://www.activequerybuilder.com/trequest.html?request=net) from the product web site
- Get it by installing the [Active Query Builder for .NET - WinForms NuGet package](https://www.nuget.org/packages/ActiveQueryBuilder.View.WinForms/) 

## What are the Metadata Providers for?
Metadata Providers are intended to link Active Query Builder to specific data access components. Their primary task is to execute the metadata retrieval queries sent by the component. The Metadata provider does not create database connectivity objects, but an instance of appropriate database connection object should be assigned to the Connection property of the metadata provider.

Read more: [What are the Syntax and Metadata providers for?](https://support.activequerybuilder.com/hc/en-us/articles/115001063445-What-are-the-Syntax-and-Metadata-providers-for-)

##### Prerequisites:
- Visual Studio 2012 or higher,
- .NET Framework 4.0 or higher.
- Active Query Builder 3 (any edition) installed on your hard drive.

## How to use this repository?

1. Clone this repository to your PC.
2. Open the "**MetadataProviders.sln**" solution.
3. Find the needed metadata provider project.
4. Update the reference to the ActiveQueryBuilder.Core assembly with the version of Active Query Builder installed on your PC.
5. Update the reference to appropriate DB Connector assembly for this metadata provider.
6. Build your metadata provider assembly.

Some of the metadata providers refer to NuGet packages of appropriate DB Connector assemblies. In case of any problems with the packages, open the "Tools" - "NuGet Package Manager" - "Package Manager Console" menu item and install them by running the following command: 

    Install-Package <Name-of-DB-Connector-NuGet-package>

## Have a question or want to leave feedback?

Welcome to the [Active Query Builder Help Center](https://support.activequerybuilder.com/hc/)!
There you will find:
- End-user's Guide,
- Getting Started guides,
- Knowledge Base,
- Community Forum.

## License
The source code of the metadata providers in this repository is covered by the [Apache license 2.0](https://www.apache.org/licenses/LICENSE-2.0).
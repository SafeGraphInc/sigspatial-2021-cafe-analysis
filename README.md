# SafeGraph SIGSPATIAL 2021 Workshop

## References

* [Understand Consumer Behavior With Verified Foot Traffic Data](https://www.safegraph.com/events/safegraph-sigspatial-2021)
* [SIGSPATIAL 2021](https://sigspatial2021.sigspatial.org/) 

## TL;DR

Fastest way to get up and running

1. Clone the repository
```shell
git clone https://github.com/SafeGraphInc/sigspatial-2021-cafe-analysis.git
```

You should see something like the following......

```
Cloning into 'sigspatial-2021-cafe-analysis'...
remote: Enumerating objects: 19, done.
remote: Counting objects: 100% (19/19), done.
remote: Compressing objects: 100% (15/15), done.
remote: Total 19 (delta 3), reused 16 (delta 2), pack-reused 0
Receiving objects: 100% (19/19), 15.05 KiB | 7.53 MiB/s, done.
Resolving deltas: 100% (3/3), done.

```

2. Change into the project directory and run the setup

```shell
cd sigspatial-2021-cafe-analysis && make setup
```

This downloads all required data, creates a virtual environment with dependencies, and starts a jupyter notebook

You should see something like the following....

```
Cloning into 'sigspatial-2021-cafe-analysis'...
remote: Enumerating objects: 19, done.
remote: Counting objects: 100% (19/19), done.
remote: Compressing objects: 100% (15/15), done.
remote: Total 19 (delta 3), reused 16 (delta 2), pack-reused 0
Receiving objects: 100% (19/19), 15.05 KiB | 7.53 MiB/s, done.
Resolving deltas: 100% (3/3), done.
..... more downloads, etc. ......
[I 2021-11-01 12:58:58.249 LabApp] JupyterLab application directory is /Users/jkyle/Projects/scratch/sigspatial-2021-cafe-analysis/venv/share/jupyter/lab
[I 12:58:58.253 NotebookApp] Serving notebooks from local directory: /Users/jkyle/Projects/scratch/sigspatial-2021-cafe-analysis
[I 12:58:58.253 NotebookApp] Jupyter Notebook 6.4.5 is running at:
[I 12:58:58.254 NotebookApp] http://localhost:8889/?token=dd81ed9a8dc1f12cb2745dc5ea1995e331f9164492b4d40a
[I 12:58:58.254 NotebookApp]  or http://127.0.0.1:8889/?token=dd81ed9a8dc1f12cb2745dc5ea1995e331f9164492b4d40a
[I 12:58:58.254 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 12:58:58.257 NotebookApp]

    To access the notebook, open this file in a browser:
        file:///Users/jkyle/Library/Jupyter/runtime/nbserver-48530-open.html
    Or copy and paste one of these URLs:
        http://localhost:8889/?token=dd81ed9a8dc1f12cb2745dc5ea1995e331f9164492b4d40a
     or http://127.0.0.1:8889/?token=dd81ed9a8dc1f12cb2745dc5ea1995e331f9164492b4d40a

```

3. Navigate to the jupyter notebook session, e.g. `http://127.0.0.1:8889/?token=dd81ed9a8dc1f12cb2745dc5ea1995e331f9164492b4d40a` in the example.

4. You're ready!

## Understand Consumer Behavior With Verified Foot Traffic Data

**Background** Visitor and demographic aggregation data provide essential context on population behavior. How often a place of interest is visited, how long do visitors stay, where did they come from, and where are they going? The answers are invaluable in numerous industries. Building financial indicators, city and urban planning, public health indicators, or identifying your primary business competitors all require accurate, high quality population and POI data. 

**Objective** Our workshop’s objective is to provide professionals, researchers, and practitioners interested in deriving human movement patterns from location data. We use a sample of our Weekly and Monthly Patterns and Core Places products to perform market research on a potential new coffee shop location. We’ll address these concerns and more in building a market analysis proposal in real time. 

**Questions to Answer** 
- How far are customers willing to travel for coffee? 
- What location will receive the most visibility? 
- Where do most of the coffee customers come from? 

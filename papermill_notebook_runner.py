import papermill as pm
import sys
import json

print("Entering python papermill runner script !!")
print("arguments are: {}, {}, {}".format(sys.argv[1], sys.argv[2], sys.argv[3]))
# Check if parameters exist

if sys.argv[3]:
    pm.execute_notebook(
     sys.argv[1],
     sys.argv[2],
     parameters = json.loads(sys.argv[3])
    )
else:
    pm.execute_notebook(
     sys.argv[1],
     sys.argv[2]
    )
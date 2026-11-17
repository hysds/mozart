cwlVersion: v1.2
class: Workflow
id: tiny
$namespaces:
  hysds: https://github.com/hysds/hysds_commons/cwl/v1#
  s: https://schema.org/
s:version: "1.0.0"
inputs:
  x: string
outputs:
  r:
    type: File
    outputSource: a/f
steps:
  a:
    run:
      class: Operation
      inputs:
        x: string
      outputs:
        f: File
      hints:
        hysds:JobType:
          job_type: "job-a:v1"
    in:
      x: x
    out: [f]

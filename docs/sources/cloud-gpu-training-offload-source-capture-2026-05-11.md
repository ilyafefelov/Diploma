# Cloud GPU Training Offload Source Capture

Date: 2026-05-11

Purpose: current-source capture for deciding whether Smart Energy Arbitrage
official NBEATSx/TFT and later DFL/DT training should run on the local GTX
1050 Ti, free notebook GPUs, or paid cloud GPU infrastructure.

## Local GPU

- NVIDIA GeForce GTX 1050 Ti: 768 CUDA cores and 4 GB GDDR5 memory according
  to NVIDIA 10-series specifications:
  <https://www.nvidia.com/en-gb/geforce/10-series/10-series-specs/>.
- Public PyTorch/NVIDIA support discussions identify GTX 1050 Ti as Pascal
  compute capability 6.1. This is usable for many PyTorch CUDA builds, but it is
  old, has no Tensor Cores, and only 4 GB VRAM:
  <https://discuss.pytorch.org/t/help-installing-pytorch-with-gtx-1050-ti/168328>.

Interpretation for this thesis repo: the 1050 Ti can be used for small smoke
training and CUDA plumbing checks, but it is not a good target for long
rolling-origin TFT/NBEATSx training or future DT sweeps.

## Hugging Face

- Hugging Face Jobs documentation says Jobs run Docker-image commands on HF
  infrastructure, support CPU/GPU/TPU hardware, and are pay-as-you-go by seconds
  used:
  <https://huggingface.co/docs/hub/jobs>.
- HF Jobs guide says Jobs require a Pro user or Team/Enterprise organization,
  support `hf jobs run`, Docker images, hardware flavors, environment variables,
  secrets, and UV scripts:
  <https://huggingface.co/docs/huggingface_hub/en/guides/jobs>.
- Hugging Face Spaces GPU pricing lists T4-small at $0.40/hour, T4-medium at
  $0.60/hour, L4 at $0.80/hour, A10G-small at $1.00/hour, and A100-large at
  $2.50/hour:
  <https://huggingface.co/docs/hub/main/en/spaces-gpus>.

Interpretation: HF Jobs are the cleanest future packaging target if the repo
gets a single training command that writes artifacts back to a Hub dataset or
downloads a compact input bundle. Spaces are better for demos than unattended
training because GPU Spaces bill while starting/running.

## Free Or Low-Cost Notebooks

- Google Colab FAQ says Colab offers free notebook access including GPUs/TPUs,
  but resources are not guaranteed, limits fluctuate, and free runtimes can be
  terminated. It also says free notebooks can run at most 12 hours depending on
  availability and usage patterns:
  <https://research.google.com/colaboratory/faq.html>.
- Kaggle notebooks provide free GPU accelerators such as T4/P100 in notebooks,
  but with quota/availability limits. Official Kaggle help is sparse, so use
  this as opportunistic compute rather than reproducible evidence infrastructure:
  <https://kaggle.zendesk.com/hc/en-us>.

Interpretation: Colab/Kaggle are useful for ad hoc experiments and quick
notebooks, but they are weak fits for reproducible Dagster/Postgres evidence
unless the training is packaged as a standalone script with resumable outputs.

## Paid GPU Clouds

- Modal pricing lists per-second GPU billing with starter free credits. Current
  page values imply roughly T4 $0.59/hour, L4 $0.80/hour, A10 $1.10/hour, A100
  40 GB $2.10/hour, and A100 80 GB $2.50/hour after converting seconds to
  hours:
  <https://modal.com/pricing>.
- Lambda pricing lists self-serve GPU instances including RTX 6000 at
  $0.69/hour, A10 at $1.29/hour, A6000 at $1.09/hour, A100 PCIe 40 GB at
  $1.99/hour, and H100 PCIe at $3.29/hour:
  <https://lambda.ai/pricing>.
- Paperspace/DigitalOcean pricing lists managed GPU machines; examples include
  A4000 $0.76/hour, RTX5000 $0.82/hour, A5000 $1.38/hour, and V100
  $2.30/hour:
  <https://www.paperspace.com/pricing>.
- RunPod pricing page exposes marketplace GPU pods and serverless pricing, but
  the public page is dynamic and did not provide stable extracted pod prices in
  this capture:
  <https://www.runpod.io/pricing>.
- Vast.ai pricing is a live marketplace and should be checked at run time:
  <https://vast.ai/pricing>.

Interpretation: for this project, low-friction paid options are Modal for
script-like jobs, Lambda/Paperspace for stable VM/notebook work, and RunPod/Vast
for cheapest marketplace GPUs if manual reliability checks are acceptable.

## Recommendation

Use a three-tier compute strategy:

1. Local CPU/GTX 1050 Ti: smoke tests, tiny latest-window screens, CUDA sanity.
2. Cheap notebook/cloud screen: latest 18/36 anchors with reduced budgets.
3. Paid reproducible run: only after screening is promising, run 104-anchor
   source-specific promotion evidence on one selected source model.

Do not spend paid GPU time on full official NBEATSx/TFT robustness until
latest-window screening shows the source is close to or better than
`strict_similar_day`.

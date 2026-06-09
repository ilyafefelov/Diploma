# Hugging Face Transformers Decision Transformer Documentation

Source URL: <https://huggingface.co/docs/transformers/model_doc/decision_transformer>

Accessed: 2026-05-10

## Source Type

Official Hugging Face Transformers documentation page for the Decision Transformer model.

## Thesis Relevance

This source documents the practical model interface for Decision Transformer in
the Hugging Face ecosystem. It is useful for implementation planning because it
connects the original Decision Transformer paper to an available library API,
including the model/config/tokenization assumptions needed for an offline
sequence-modeling experiment.

## Short Summary

Decision Transformer frames offline reinforcement learning as conditional
sequence modeling. Instead of learning a value function or running online
exploration, the model conditions on previous states, actions, and desired
return-to-go, then predicts future actions. In this thesis, that makes Decision
Transformer a future offline research primitive for BESS dispatch trajectories,
not a current live controller.

## Claim Boundary

- Include as implementation reference for a future offline DT candidate.
- Do not cite as evidence that the current project has a deployed Decision
  Transformer controller.
- Do not use it to weaken the frozen `strict_similar_day` LP/oracle promotion
  gate.

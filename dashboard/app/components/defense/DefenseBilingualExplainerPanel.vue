<script setup lang="ts">
import { CURRENT_BILINGUAL_STRATEGY_EXPLAINER } from '~/utils/defenseDataset'
</script>

<template>
  <section class="bilingual-explainer-panel">
    <div class="section-heading">
      <div>
        <p class="eyebrow">
          Bilingual strategy explainer
        </p>
        <h2>Current offline V2+, DT/LAVA next</h2>
        <p class="section-explainer">
          A plain-language explanation for ML reviewers and thesis defense: what V2+ is, why it is not live execution,
          how data moves through the pipeline, and how this becomes the teacher path for DT/LAVA.
        </p>
      </div>
      <span class="source-pill">EN / UA</span>
    </div>
    <div class="bilingual-explainer-grid">
      <article
        v-for="section in CURRENT_BILINGUAL_STRATEGY_EXPLAINER"
        :key="section.label"
        class="bilingual-explainer-card"
      >
        <header>
          <span>{{ section.label }}</span>
        </header>
        <div class="language-columns">
          <div class="language-column">
            <p class="language-kicker">
              English
            </p>
            <h3>{{ section.englishTitle }}</h3>
            <p>{{ section.englishBody }}</p>
            <ul>
              <li
                v-for="bullet in section.englishBullets"
                :key="`en-${section.label}-${bullet}`"
              >
                {{ bullet }}
              </li>
            </ul>
          </div>
          <div class="language-column language-column--ua">
            <p class="language-kicker">
              Українською
            </p>
            <h3>{{ section.ukrainianTitle }}</h3>
            <p>{{ section.ukrainianBody }}</p>
            <ul>
              <li
                v-for="bullet in section.ukrainianBullets"
                :key="`ua-${section.label}-${bullet}`"
              >
                {{ bullet }}
              </li>
            </ul>
          </div>
        </div>
      </article>
    </div>
  </section>
</template>

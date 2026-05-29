import { renderQaSlide, SLIDES } from "./common.mjs";

export async function slide03(presentation, ctx) {
  return renderQaSlide(presentation, ctx, SLIDES[2]);
}

import path from "node:path";

export async function renderFullImageSlide(presentation, ctx, slideNumber) {
  const slide = presentation.slides.add();
  const imagePath = path.join(ctx.workspaceDir, "images", `slide-${String(slideNumber).padStart(2, "0")}.png`);
  await ctx.addImage(slide, {
    path: imagePath,
    x: 0,
    y: 0,
    width: ctx.W,
    height: ctx.H,
    fit: "cover",
    alt: `Image-generated slide ${slideNumber}`,
  });
  return slide;
}

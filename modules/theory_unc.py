import awkward as ak
import numpy as np
import variation as variation_module

def theory_unc(events, variations, cfg):
    doTheoryVariations = cfg.get("do_theory_variations", True)
    if doTheoryVariations:
        # QCD Scale
        nVariations = len(events.LHEScaleWeight[0])
        for i, j in enumerate(
            [
                0,
                1,
                2,
                3,
                nVariations - 1,
                nVariations - 2,
                nVariations - 3,
                nVariations - 4,
            ]
        ):
            var_name = f"QCDscale_{i}"

            varied_col = variation_module.Variation.format_varied_column(
                "weight", var_name
            )

            events[varied_col] = events.weight * events.LHEScaleWeight[:, j]

            variations.register_variation(["weight"], var_name)


        # Pdf Weights
        nVariations = len(events.LHEPdfWeight[0])
        for i, j in enumerate(range(nVariations)):
            #events[f"weight_pdfWeight_{i}"] = events.weight * events.LHEPdfWeight[:, j]

            var_name = f"PdfWeight_{i}"

            varied_col = variation_module.Variation.format_varied_column(
                "weight", var_name
            )

            events[varied_col] = events.weight * events.LHEPdfWeight[:, j]

            variations.register_variation(["weight"], var_name)

    return events, variations

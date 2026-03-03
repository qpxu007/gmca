# source https://www.desy.de/~twhite/crystfel/twin-calculator.pdf

crystallography_data = {
    "Lattices": {
        "Triclinic": {
            "Point Groups": ["1"],
            "Space Groups": ["P1", "P  1"]
        },
        "Monoclinic": {
            "Point Groups": ["m"],
            "Space Groups": ["Pm", "Pc", "Cm", "Cc", "2", "2/m", "P2", "P21", "C2", "P2/m", "P21/m", "C2/m", "P2/c",
                             "P21/c", "C2/c"]
        },
        "Orthorhombic": {
            "Point Groups": ["mm2", "222", "mmm"],
            "Space Groups": {
                "mm2": ["Pmm2", "Pmc21", "Pcc2", "Pma2", "Pca21", "Pnc2", "Pmn21", "Pba2", "Pna21", "Pnn2", "Cmm2",
                        "Cmc21", "Ccc2", "Amm2", "Aem2", "Ama2", "Aea2", "Fmm2", "Fdd2", "Imm2", "Iba2", "Ima2"],
                "222": ["P222", "P2221", "P21212", "P212121", "C2221", "C222", "F222", "I222", "I212121"],
                "mmm": ["Pmmm", "Pnnn", "Pccm", "Pban", "Pmma", "Pnna", "Pmna", "Pcca", "Pbam", "Pccn", "Pbcm", "Pnnm",
                        "Pmmn", "Pbcn", "Pbca", "Pnma", "Cmcm", "Cmce", "Cmmm", "Cccm", "Cmme", "Ccce", "Fmmm", "Fddd",
                        "Immm", "Ibam", "Ibca", "Imma"]
            }
        },
        "Tetragonal": {
            "Point Groups": ["4", "4mm", "42m", "4m2", "4/m", "422", "4/mmm"],
            "Space Groups": {
                "4": ["P4", "P41", "P42", "P43", "I4", "I41"],
                "4mm": ["P4mm", "P4bm", "P42cm", "P42nm", "P4cc", "P4nc", "P42mc", "P42bc", "I4mm", "I4cm", "I41md",
                        "I41cd"],
                "42m": ["P42m", "P42c", "P421m", "P421c", "I42m", "I42d"],
                "4m2": ["P4m2", "P4c2", "P4b2", "P4n2", "I4m2", "I4c2"],
                "4/m": ["P4/m", "P42/m", "P4/n", "P42/n", "I4/m", "I41/a"],
                "422": ["P422", "P4212", "P4122", "P41212", "P4222", "P42212", "P4322", "P43212", "I422", "I4122"],
                "4/mmm": ["P4/mmm", "P4/mcc", "P4/nbm", "P4/nnc", "P4/mbm", "P4/mnc", "P4/nmm", "P4/ncc", "P42/mmc",
                          "P42/mcm", "P42/nbc", "P42/nnm", "P42/mbc", "P42/mnm", "P42/nmc", "P42/ncm", "I4/mmm",
                          "I4/mcm", "I41/amd", "I41/acd"]
            }
        },
        "Rhombohedral": {
            "Point Groups": ["3", "3m", "32"],
            "Space Groups": {
                "3": ["R3 (H3)", "R  3   (H3)"],
                "3m": ["R3m (H3m)", "R3c (H3c)"],
                "32": ["R32 (H32)", "R  3  m (H3m)", "R  3  c (H3c)"]
            }
        },
        "Hexagonal": {
            "Point Groups": ["3", "6mm", "6", "312", "3m1", "6m2", "622"],
            "Space Groups": {
                "3": ["P3", "P31", "P32", "P  3  ", "P6mm", "P6cc", "P63cm", "P63mc"],
                "6": ["P6", "P61", "P65", "P62", "P64", "P63", "P312", "P3112", "P3212", "P321", "P3121", "P3221",
                      "P6/m", "P63/m"],
                "3m1": ["P3m1", "P3c1", "P6", "P31m", "P31c"],
                "6m2": ["P6m2", "P6c2", "P62m", "P62c", "P  3  1m", "P  3  1c"],
                "622": ["P622", "P6122", "P6522", "P6222", "P6422", "P6322", "P6/mmm", "P6/mcc", "P63/mcm", "P63/mmc"]
            }
        },
        "Cubic": {
            "Point Groups": ["23", "43m", "432"],
            "Space Groups": {
                "23": ["P23", "F23", "I23", "P213", "I213"],
                "43m": ["P43m", "F43m", "I43m", "P43n", "F43c", "I43d"],
                "432": ["P432", "P4232", "F432", "F4132", "I432", "P4332", "P4132", "I4132"]
            }
        }
    },
    "Laue Classes": ["1", "3", "6/m", "6", "2/m", "m", "3m", "32", "6/mmm", "622", "6m2", "62m", "6mm", "mmm", "222",
                     "mm2", "3m1", "321", "3m1", "432", "43m", "4/mmm", "422", "42m", "4m2", "4mm"]
}

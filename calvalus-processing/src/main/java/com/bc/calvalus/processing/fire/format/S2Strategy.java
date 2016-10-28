package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.CompositingType;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.VariableConfig;

import java.time.Year;

class S2Strategy implements SensorStrategy {

    private final PixelProductAreaProvider areaProvider;

    S2Strategy() {
        areaProvider = new S2PixelProductAreaProvider();
    }

    @Override
    public PixelProductArea getArea(String identifier) {
        return areaProvider.getArea(identifier);
    }

    @Override
    public PixelProductArea[] getAllAreas() {
        return areaProvider.getAllAreas();
    }

    @Override
    public Workflow getWorkflow(WorkflowConfig workflowConfig) {
        Workflow workflow = new Workflow.Sequential();
        workflow.setSustainable(false);
        Configuration jobConfig = workflowConfig.jobConfig;
        String area = workflowConfig.area;
        String year = workflowConfig.year;
        String month = workflowConfig.month;
        String outputDir = workflowConfig.outputDir;
        String userName = workflowConfig.userName;
        String productionName = workflowConfig.productionName;
        HadoopProcessingService processingService = workflowConfig.processingService;

        PixelProductArea pixelProductArea = getArea(area);

        BinningConfig l3Config = getBinningConfig();
        String l3ConfigXml = l3Config.toXml();
        GeometryFactory gf = new GeometryFactory();
        Geometry regionGeometry = new Polygon(new LinearRing(new PackedCoordinateSequence.Float(new double[]{
                pixelProductArea.left - 180, pixelProductArea.top - 90,
                pixelProductArea.right - 180, pixelProductArea.top - 90,
                pixelProductArea.right - 180, pixelProductArea.bottom - 90,
                pixelProductArea.left - 180, pixelProductArea.bottom - 90,
                pixelProductArea.left - 180, pixelProductArea.top - 90
        }, 2), gf), new LinearRing[0], gf);

        String tilesSpec = "(" + pixelProductArea.tiles.replace(",", "|").replace(" ", "").trim() + ")";

        String inputPathPattern = String.format("hdfs://calvalus/calvalus/projects/fire/s2-ba/.*/BA-T%s-%s%s.*.nc", tilesSpec, year, month);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, inputPathPattern);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, area);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry.toString());

        int day = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atEndOfMonth().getDayOfMonth();
        String minDate = String.format("%s-%s-01", year, month);
        String maxDate = String.format("%s-%s-%s", year, month, day);
        jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, minDate);
        jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, maxDate);

        WorkflowItem item = new L3WorkflowItem(processingService, userName, productionName, jobConfig);
        workflow.add(item);

        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir + "_format");
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, outputDir + "_format");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4-CF");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz");

        WorkflowItem formatItem = new L3FormatWorkflowItem(processingService,
                userName,
                productionName + " Format", jobConfig);
        workflow.add(formatItem);

        return workflow;
    }

    private static BinningConfig getBinningConfig() {
        BinningConfig binningConfig = new BinningConfig();
        binningConfig.setCompositingType(CompositingType.BINNING);
        binningConfig.setNumRows(1001878);
        binningConfig.setSuperSampling(1);
        binningConfig.setMaskExpr("true");
        VariableConfig doyConfig = new VariableConfig("day_of_year", "JD");
        VariableConfig clConfig = new VariableConfig("confidence_level", "CL");
        binningConfig.setVariableConfigs(doyConfig, clConfig);
        binningConfig.setPlanetaryGrid("org.esa.snap.binning.support.PlateCarreeGrid");
        AggregatorConfig aggConfig = new JDAggregator.Config("day_of_year", "confidence_level");
        binningConfig.setAggregatorConfigs(aggConfig);
        return binningConfig;
    }

    private static class S2PixelProductAreaProvider implements PixelProductAreaProvider {

        private enum S2PixelProductArea {
            AREA_1(154, 90, 159, 95, 1, "area_1", "26NQG, 26NQH, 26NQJ, 26NQK, 26NRG, 26NRH, 26NRJ, 26NRK, 27NTB, 27NTC, 27NTD, 27NTE, 27NUB, 27NUC, 27NUD, 27NUE"),
            AREA_2(159, 90, 164, 95, 2, "area_2", "27NXB, 27NXC, 27NXD, 27NXE, 27NYB, 27NYC, 27NYD, 27NYE, 27NZB, 27NZC, 27NZD, 27NZE, 28NBG, 28NBH, 28NBJ, 28NBK"),
            AREA_3(164, 90, 169, 95, 3, "area_3", "28NDG, 28NDH, 28NDJ, 28NDK, 28NEG, 28NEH, 28NEJ, 28NEK, 28NFG, 28NFH, 28NFJ, 28NFK, 28NGG, 28NGH, 28NGJ, 28NGK, 28NHG, 28NHH, 28NHJ, 28NHK"),
            AREA_4(169, 90, 174, 95, 4, "area_4", "29NLB, 29NLC, 29NLD, 29NLE, 29NMB, 29NMC, 29NMD, 29NME, 29NNB, 29NNC, 29NND, 29NNE, 29NPB, 29NPC, 29NPD, 29NPE, 29NQB, 29NQC, 29NQD, 29NQE"),
            AREA_5(174, 90, 179, 95, 5, "area_5", "30NTG, 30NTH, 30NTJ, 30NTK, 30NUG, 30NUH, 30NUJ, 30NUK, 30NVG, 30NVH, 30NVJ, 30NVK, 30NWG, 30NWH, 30NWJ, 30NWK, 30NXG, 30NXH, 30NXJ, 30NXK"),
            AREA_6(179, 90, 184, 95, 6, "area_6", "30NZG, 30NZH, 30NZJ, 30NZK, 31NBB, 31NBC, 31NBD, 31NBE, 31NCB, 31NCC, 31NCD, 31NCE, 31NDB, 31NDC, 31NDD, 31NDE, 31NEB, 31NEC, 31NED, 31NEE"),
            AREA_7(184, 90, 189, 95, 7, "area_7", "31NGB, 31NGC, 31NGD, 31NGE, 31NHB, 31NHC, 31NHD, 31NHE, 32NKG, 32NKH, 32NKJ, 32NKK, 32NLG, 32NLH, 32NLJ, 32NLK"),
            AREA_8(189, 90, 194, 95, 8, "area_8", "32NPG, 32NPH, 32NPJ, 32NPK, 32NQG, 32NQH, 32NQJ, 32NQK, 32NRG, 32NRH, 32NRJ, 32NRK, 33NTB, 33NTC, 33NTD, 33NTE"),
            AREA_9(194, 90, 199, 95, 9, "area_9", "33NVB, 33NVC, 33NVD, 33NVE, 33NWB, 33NWC, 33NWD, 33NWE, 33NXB, 33NXC, 33NXD, 33NXE, 33NYB, 33NYC, 33NYD, 33NYE, 33NZB, 33NZC, 33NZD, 33NZE"),
            AREA_10(199, 90, 204, 95, 10, "area_10", "34NCG, 34NCH, 34NCJ, 34NCK, 34NDG, 34NDH, 34NDJ, 34NDK, 34NEG, 34NEH, 34NEJ, 34NEK, 34NFG, 34NFH, 34NFJ, 34NFK, 34NGG, 34NGH, 34NGJ, 34NGK"),
            AREA_11(204, 90, 209, 95, 11, "area_11", "35NKB, 35NKC, 35NKD, 35NKE, 35NLB, 35NLC, 35NLD, 35NLE, 35NMB, 35NMC, 35NMD, 35NME, 35NNB, 35NNC, 35NND, 35NNE, 35NPB, 35NPC, 35NPD, 35NPE"),
            AREA_12(209, 90, 214, 95, 12, "area_12", "35NRB, 35NRC, 35NRD, 35NRE, 36NTG, 36NTH, 36NTJ, 36NTK, 36NUG, 36NUH, 36NUJ, 36NUK, 36NVG, 36NVH, 36NVJ, 36NVK, 36NWG, 36NWH, 36NWJ, 36NWK"),
            AREA_13(214, 90, 219, 95, 13, "area_13", "36NYG, 36NYH, 36NYJ, 36NYK, 36NZG, 36NZH, 36NZJ, 36NZK, 37NBB, 37NBC, 37NBD, 37NBE, 37NCB, 37NCC, 37NCD, 37NCE"),
            AREA_14(219, 90, 224, 95, 14, "area_14", "37NFB, 37NFC, 37NFD, 37NFE, 37NGB, 37NGC, 37NGD, 37NGE, 37NHB, 37NHC, 37NHD, 37NHE, 38NKG, 38NKH, 38NKJ, 38NKK"),
            AREA_15(224, 90, 229, 95, 15, "area_15", "38NMG, 38NMH, 38NMJ, 38NMK, 38NNG, 38NNH, 38NNJ, 38NNK, 38NPG, 38NPH, 38NPJ, 38NPK, 38NQG, 38NQH, 38NQJ, 38NQK, 38NRG, 38NRH, 38NRJ, 38NRK"),
            AREA_16(229, 90, 233, 95, 16, "area_16", "39NUB, 39NUC, 39NUD, 39NUE, 39NVB, 39NVC, 39NVD, 39NVE, 39NWB, 39NWC, 39NWD, 39NWE, 39NXB, 39NXC, 39NXD, 39NXE"),
            AREA_17(154, 95, 159, 100, 17, "area_17", "26NQM, 26NQN, 26NQP, 26NRM, 26NRN, 26NRP, 26PQQ, 26PQR, 26PRQ, 26PRR, 27NTG, 27NTH, 27NTJ, 27NUG, 27NUH, 27NUJ, 27PTK, 27PTL, 27PUK, 27PUL"),
            AREA_18(159, 95, 164, 100, 18, "area_18", "27NXG, 27NXH, 27NXJ, 27NYG, 27NYH, 27NYJ, 27NZG, 27NZH, 27NZJ, 27PXK, 27PXL, 27PYK, 27PYL, 27PZK, 27PZL, 28NBM, 28NBN, 28NBP, 28PBQ, 28PBR"),
            AREA_19(164, 95, 169, 100, 19, "area_19", "28NDM, 28NDN, 28NDP, 28NEM, 28NEN, 28NEP, 28NFM, 28NFN, 28NFP, 28NGM, 28NGN, 28NGP, 28NHM, 28NHN, 28NHP, 28PDQ, 28PDR, 28PEQ, 28PER, 28PFQ, 28PFR, 28PGQ, 28PGR, 28PHQ, 28PHR"),
            AREA_20(169, 95, 174, 100, 20, "area_20", "29NLG, 29NLH, 29NLJ, 29NMG, 29NMH, 29NMJ, 29NNG, 29NNH, 29NNJ, 29NPG, 29NPH, 29NPJ, 29NQG, 29NQH, 29NQJ, 29PLK, 29PLL, 29PMK, 29PML, 29PNK, 29PNL, 29PPK, 29PPL, 29PQK, 29PQL"),
            AREA_21(174, 95, 179, 100, 21, "area_21", "30NTM, 30NTN, 30NTP, 30NUM, 30NUN, 30NUP, 30NVM, 30NVN, 30NVP, 30NWM, 30NWN, 30NWP, 30NXM, 30NXN, 30NXP, 30PTQ, 30PTR, 30PUQ, 30PUR, 30PVQ, 30PVR, 30PWQ, 30PWR, 30PXQ, 30PXR"),
            AREA_22(179, 95, 184, 100, 22, "area_22", "30NZM, 30NZN, 30NZP, 30PZQ, 30PZR, 31NBG, 31NBH, 31NBJ, 31NCG, 31NCH, 31NCJ, 31NDG, 31NDH, 31NDJ, 31NEG, 31NEH, 31NEJ, 31PBK, 31PBL, 31PCK, 31PCL, 31PDK, 31PDL, 31PEK"),
            AREA_23(184, 95, 189, 100, 23, "area_23", "31NGG, 31NGH, 31NGJ, 31NHG, 31NHH, 31NHJ, 31PGK, 31PGL, 31PHK, 31PHL, 32NKM, 32NKN, 32NKP, 32NLM, 32NLN, 32NLP, 32PKQ, 32PKR, 32PLQ, 32PLR"),
            AREA_24(189, 95, 194, 100, 24, "area_24", "32NPM, 32NPN, 32NPP, 32NQM, 32NQN, 32NQP, 32NRM, 32NRN, 32NRP, 32PPQ, 32PPR, 32PQQ, 32PQR, 32PRQ, 32PRR, 33NTG, 33NTH, 33NTJ, 33PTK, 33PTL"),
            AREA_25(194, 95, 199, 100, 25, "area_25", "33NVG, 33NVH, 33NVJ, 33NWG, 33NWH, 33NWJ, 33NXG, 33NXH, 33NXJ, 33NYG, 33NYH, 33NYJ, 33NZG, 33NZH, 33NZJ, 33PVK, 33PVL, 33PWK, 33PWL, 33PXK, 33PXL, 33PYK, 33PYL, 33PZK, 33PZL"),
            AREA_26(199, 95, 204, 100, 26, "area_26", "34NCM, 34NCN, 34NCP, 34NDM, 34NDN, 34NDP, 34NEM, 34NEN, 34NEP, 34NFM, 34NFN, 34NFP, 34NGM, 34NGN, 34NGP, 34PCQ, 34PCR, 34PDQ, 34PDR, 34PEQ, 34PER, 34PFQ, 34PFR, 34PGQ, 34PGR"),
            AREA_27(204, 95, 209, 100, 27, "area_27", "35NKG, 35NKH, 35NKJ, 35NLG, 35NLH, 35NLJ, 35NMG, 35NMH, 35NMJ, 35NNG, 35NNH, 35NNJ, 35NPG, 35NPH, 35NPJ, 35PKK, 35PKL, 35PLK, 35PLL, 35PMK, 35PML, 35PNK, 35PNL, 35PPK, 35PPL"),
            AREA_28(209, 95, 214, 100, 28, "area_28", "35NRG, 35NRH, 35NRJ, 35PRK, 35PRL, 36NTM, 36NTN, 36NTP, 36NUM, 36NUN, 36NUP, 36NVM, 36NVN, 36NVP, 36NWM, 36NWN, 36NWP, 36PTQ, 36PTR, 36PUQ, 36PUR, 36PVQ, 36PVR, 36PWQ"),
            AREA_29(214, 95, 219, 100, 29, "area_29", "36NYM, 36NYN, 36NYP, 36NZM, 36NZN, 36NZP, 36PYQ, 36PYR, 36PZQ, 36PZR, 37NBG, 37NBH, 37NBJ, 37NCG, 37NCH, 37NCJ, 37PBK, 37PBL, 37PCK, 37PCL"),
            AREA_30(219, 95, 224, 100, 30, "area_30", "37NFG, 37NFH, 37NFJ, 37NGG, 37NGH, 37NGJ, 37NHG, 37NHH, 37NHJ, 37PFK, 37PFL, 37PGK, 37PGL, 37PHK, 37PHL, 38NKM, 38NKN, 38NKP, 38PKQ, 38PKR"),
            AREA_31(224, 95, 229, 100, 31, "area_31", "38NMM, 38NMN, 38NMP, 38NNM, 38NNN, 38NNP, 38NPM, 38NPN, 38NPP, 38NQM, 38NQN, 38NQP, 38NRM, 38NRN, 38NRP, 38PMQ, 38PMR, 38PNQ, 38PNR, 38PPQ, 38PPR, 38PQQ, 38PQR, 38PRQ, 38PRR"),
            AREA_32(229, 95, 233, 100, 32, "area_32", "39NUG, 39NUH, 39NUJ, 39NVG, 39NVH, 39NVJ, 39NWG, 39NWH, 39NWJ, 39NXG, 39NXH, 39NXJ, 39PUK, 39PUL, 39PVK, 39PVL, 39PWK, 39PWL, 39PXK, 39PXL"),
            AREA_33(154, 100, 159, 105, 33, "area_33", "26PQA, 26PQT, 26PQU, 26PQV, 26PRA, 26PRT, 26PRU, 26PRV, 27PTN, 27PTP, 27PTQ, 27PTR, 27PUN, 27PUP, 27PUQ, 27PUR"),
            AREA_34(159, 100, 164, 105, 34, "area_34", "27PXN, 27PXP, 27PXQ, 27PXR, 27PYN, 27PYP, 27PYQ, 27PYR, 27PZN, 27PZP, 27PZQ, 27PZR, 28PBA, 28PBT, 28PBU, 28PBV"),
            AREA_35(164, 100, 169, 105, 35, "area_35", "28PDA, 28PDT, 28PDU, 28PDV, 28PEA, 28PET, 28PEU, 28PEV, 28PFA, 28PFT, 28PFU, 28PFV, 28PGA, 28PGT, 28PGU, 28PGV, 28PHA, 28PHT, 28PHU, 28PHV"),
            AREA_36(169, 100, 174, 105, 36, "area_36", "29PLN, 29PLP, 29PLQ, 29PLR, 29PMN, 29PMP, 29PMQ, 29PMR, 29PNN, 29PNP, 29PNQ, 29PNR, 29PPN, 29PPP, 29PPQ, 29PPR, 29PQN, 29PQP, 29PQQ, 29PQR"),
            AREA_37(174, 100, 179, 105, 37, "area_37", "30PTA, 30PTT, 30PTU, 30PTV, 30PUA, 30PUT, 30PUU, 30PUV, 30PVA, 30PVT, 30PVU, 30PVV, 30PWA, 30PWT, 30PWU, 30PWV, 30PXA, 30PXT, 30PXU, 30PXV"),
            AREA_38(179, 100, 184, 105, 38, "area_38", "30PZA, 30PZT, 30PZU, 30PZV, 31PBN, 31PBP, 31PBQ, 31PBR, 31PCN, 31PCP, 31PCQ, 31PCR, 31PDN, 31PDP, 31PDQ, 31PDR"),
            AREA_39(184, 100, 189, 105, 39, "area_39", "31PGN, 31PGP, 31PGQ, 31PGR, 31PHN, 31PHP, 31PHQ, 31PHR, 32PKA, 32PKT, 32PKU, 32PKV, 32PLA, 32PLT, 32PLU, 32PLV"),
            AREA_40(189, 100, 194, 105, 40, "area_40", "32PPA, 32PPT, 32PPU, 32PPV, 32PQA, 32PQT, 32PQU, 32PQV, 32PRA, 32PRT, 32PRU, 32PRV, 33PTN, 33PTP, 33PTQ, 33PTR"),
            AREA_41(194, 100, 199, 105, 41, "area_41", "33PVN, 33PVP, 33PVQ, 33PVR, 33PWN, 33PWP, 33PWQ, 33PWR, 33PXN, 33PXP, 33PXQ, 33PXR, 33PYN, 33PYP, 33PYQ, 33PYR, 33PZN, 33PZP, 33PZQ, 33PZR"),
            AREA_42(199, 100, 204, 105, 42, "area_42", "34PCA, 34PCT, 34PCU, 34PCV, 34PDA, 34PDT, 34PDU, 34PDV, 34PEA, 34PET, 34PEU, 34PEV, 34PFA, 34PFT, 34PFU, 34PFV, 34PGA, 34PGT, 34PGU, 34PGV"),
            AREA_43(204, 100, 209, 105, 43, "area_43", "35PKN, 35PKP, 35PKQ, 35PKR, 35PLN, 35PLP, 35PLQ, 35PLR, 35PMN, 35PMP, 35PMQ, 35PMR, 35PNN, 35PNP, 35PNQ, 35PNR, 35PPN, 35PPP, 35PPQ, 35PPR"),
            AREA_44(209, 100, 214, 105, 44, "area_44", "35PRN, 35PRP, 35PRQ, 35PRR, 36PTA, 36PTT, 36PTU, 36PTV, 36PUA, 36PUT, 36PUU, 36PUV, 36PVA, 36PVT, 36PVU, 36PVV"),
            AREA_45(214, 100, 219, 105, 45, "area_45", "36PYA, 36PYT, 36PYU, 36PYV, 36PZA, 36PZT, 36PZU, 36PZV, 37PBN, 37PBP, 37PBQ, 37PBR, 37PCN, 37PCP, 37PCQ, 37PCR"),
            AREA_46(219, 100, 224, 105, 46, "area_46", "37PFN, 37PFP, 37PFQ, 37PFR, 37PGN, 37PGP, 37PGQ, 37PGR, 37PHN, 37PHP, 37PHQ, 37PHR, 38PKA, 38PKT, 38PKU, 38PKV"),
            AREA_47(224, 100, 229, 105, 47, "area_47", "38PMA, 38PMT, 38PMU, 38PMV, 38PNA, 38PNT, 38PNU, 38PNV, 38PPA, 38PPT, 38PPU, 38PPV, 38PQA, 38PQT, 38PQU, 38PQV, 38PRA, 38PRT, 38PRU, 38PRV"),
            AREA_48(229, 100, 233, 105, 48, "area_48", "39PUN, 39PUP, 39PUQ, 39PUR, 39PVN, 39PVP, 39PVQ, 39PVR, 39PWN, 39PWP, 39PWQ, 39PWR, 39PXN, 39PXP, 39PXQ, 39PXR"),
            AREA_49(154, 105, 159, 110, 49, "area_49", "26PQC, 26PRC, 26QQD, 26QQE, 26QQF, 26QQG, 26QRD, 26QRE, 26QRF, 26QRG, 27PTT, 27PUT, 27QTA, 27QTB, 27QTU, 27QTV, 27QUA, 27QUB, 27QUU, 27QUV"),
            AREA_50(159, 105, 164, 110, 50, "area_50", "27PXT, 27PYT, 27PZT, 27QXA, 27QXB, 27QXU, 27QXV, 27QYA, 27QYB, 27QYU, 27QYV, 27QZA, 27QZB, 27QZU, 27QZV, 28PBC, 28QBD, 28QBE, 28QBF, 28QBG"),
            AREA_51(164, 105, 169, 110, 51, "area_51", "28PDC, 28PEC, 28PFC, 28PGC, 28PHC, 28QDD, 28QDE, 28QDF, 28QDG, 28QED, 28QEE, 28QEF, 28QEG, 28QFD, 28QFE, 28QFF, 28QFG, 28QGD, 28QGE, 28QGF, 28QGG, 28QHD, 28QHE, 28QHF, 28QHG"),
            AREA_52(169, 105, 174, 110, 52, "area_52", "29PLT, 29PMT, 29PNT, 29PPT, 29PQT, 29QLA, 29QLB, 29QLU, 29QLV, 29QMA, 29QMB, 29QMU, 29QMV, 29QNA, 29QNB, 29QNU, 29QNV, 29QPA, 29QPB, 29QPU, 29QPV, 29QQA, 29QQB, 29QQU, 29QQV"),
            AREA_53(174, 105, 179, 110, 53, "area_53", "30PTC, 30PUC, 30PVC, 30PWC, 30PXC, 30QTD, 30QTE, 30QTF, 30QTG, 30QUD, 30QUE, 30QUF, 30QUG, 30QVD, 30QVE, 30QVF, 30QVG, 30QWD, 30QWE, 30QWF, 30QWG, 30QXD, 30QXE, 30QXF"),
            AREA_54(179, 105, 184, 110, 54, "area_54", "30PZC, 30QZD, 30QZE, 30QZF, 30QZG, 31PBT, 31PCT, 31PDT, 31QBA, 31QBB, 31QBU, 31QBV, 31QCA, 31QCB, 31QCU, 31QCV, 31QDA, 31QDB, 31QDU, 31QDV"),
            AREA_55(184, 105, 189, 110, 55, "area_55", "31PGT, 31PHT, 31QGA, 31QGB, 31QGU, 31QGV, 31QHA, 31QHB, 31QHU, 31QHV, 32PKC, 32PLC, 32QKD, 32QKE, 32QKF, 32QKG, 32QLD, 32QLE, 32QLF, 32QLG"),
            AREA_56(189, 105, 194, 110, 56, "area_56", "32PPC, 32PQC, 32PRC, 32QPD, 32QPE, 32QPF, 32QPG, 32QQD, 32QQE, 32QQF, 32QQG, 32QRD, 32QRE, 32QRF, 32QRG, 33PTT, 33QTA, 33QTB, 33QTU, 33QTV"),
            AREA_57(194, 105, 199, 110, 57, "area_57", "33PVT, 33PWT, 33PXT, 33PYT, 33PZT, 33QVA, 33QVB, 33QVU, 33QVV, 33QWA, 33QWB, 33QWU, 33QWV, 33QXA, 33QXB, 33QXU, 33QXV, 33QYA, 33QYB, 33QYU, 33QYV, 33QZA, 33QZB, 33QZU, 33QZV"),
            AREA_58(199, 105, 204, 110, 58, "area_58", "34PCC, 34PDC, 34PEC, 34PFC, 34PGC, 34QCD, 34QCE, 34QCF, 34QCG, 34QDD, 34QDE, 34QDF, 34QDG, 34QED, 34QEE, 34QEF, 34QEG, 34QFD, 34QFE, 34QFF, 34QFG, 34QGD, 34QGE, 34QGF, 34QGG"),
            AREA_59(204, 105, 209, 110, 59, "area_59", "35PKT, 35PLT, 35PMT, 35PNT, 35PPT, 35QKA, 35QKB, 35QKU, 35QKV, 35QLA, 35QLB, 35QLU, 35QLV, 35QMA, 35QMB, 35QMU, 35QMV, 35QNA, 35QNB, 35QNU, 35QNV, 35QPA, 35QPU, 35QPV"),
            AREA_60(209, 105, 214, 110, 60, "area_60", "35PRT, 35QRA, 35QRB, 35QRU, 35QRV, 36PTC, 36PUC, 36PVC, 36QTD, 36QTE, 36QTF, 36QTG, 36QUD, 36QUE, 36QUF, 36QUG, 36QVD, 36QVE, 36QVF, 36QVG"),
            AREA_61(214, 105, 219, 110, 61, "area_61", "36PYC, 36PZC, 36QYD, 36QYE, 36QYF, 36QYG, 36QZD, 36QZE, 36QZF, 36QZG, 37PBT, 37PCT, 37QBA, 37QBB, 37QBU, 37QBV, 37QCA, 37QCB, 37QCU, 37QCV"),
            AREA_62(219, 105, 224, 110, 62, "area_62", "37PFT, 37PGT, 37PHT, 37QFA, 37QFB, 37QFU, 37QFV, 37QGA, 37QGB, 37QGU, 37QGV, 37QHA, 37QHB, 37QHU, 37QHV, 38PKC, 38QKD, 38QKE, 38QKF, 38QKG"),
            AREA_63(224, 105, 229, 110, 63, "area_63", "38PMC, 38PNC, 38PPC, 38PQC, 38PRC, 38QMD, 38QME, 38QMF, 38QMG, 38QND, 38QNE, 38QNF, 38QNG, 38QPD, 38QPE, 38QPF, 38QPG, 38QQD, 38QQE, 38QQF, 38QQG, 38QRD, 38QRE, 38QRF, 38QRG"),
            AREA_64(229, 105, 233, 110, 64, "area_64", "39PUT, 39PVT, 39PWT, 39PXT, 39QUA, 39QUB, 39QUU, 39QUV, 39QVA, 39QVB, 39QVU, 39QVV, 39QWA, 39QWB, 39QWU, 39QWV, 39QXA, 39QXU, 39QXV"),
            AREA_65(154, 110, 159, 115, 65, "area_65", "26QQJ, 26QQK, 26QQL, 26QQM, 26QRJ, 26QRK, 26QRL, 26QRM, 27QTD, 27QTE, 27QTF, 27QTG, 27QUD, 27QUE, 27QUF, 27QUG"),
            AREA_66(159, 110, 164, 115, 66, "area_66", "27QXD, 27QXE, 27QXF, 27QXG, 27QYD, 27QYE, 27QYF, 27QYG, 27QZD, 27QZE, 27QZF, 27QZG, 28QBJ, 28QBK, 28QBL, 28QBM"),
            AREA_67(164, 110, 169, 115, 67, "area_67", "28QDJ, 28QDK, 28QDL, 28QDM, 28QEJ, 28QEK, 28QEL, 28QEM, 28QFJ, 28QFK, 28QFL, 28QFM, 28QGJ, 28QGK, 28QGL, 28QGM, 28QHJ, 28QHK"),
            AREA_68(169, 110, 174, 115, 68, "area_68", "29QLD, 29QLE, 29QLF, 29QLG, 29QMD, 29QME, 29QMF, 29QMG, 29QND, 29QNE, 29QNF, 29QNG, 29QPD, 29QPE, 29QPF, 29QPG, 29QQD"),
            AREA_69(174, 110, 179, 115, 69, "area_69", "30QTJ, 30QTK, 30QTL, 30QTM, 30QUJ, 30QUK, 30QUL, 30QUM, 30QVJ, 30QVK, 30QVL, 30QVM, 30QWJ, 30QWK, 30QWL, 30QWM"),
            AREA_70(179, 110, 184, 115, 70, "area_70", "30QZJ, 30QZK, 30QZL, 30QZM, 31QBD, 31QBE, 31QBF, 31QBG, 31QCD, 31QCE, 31QCF, 31QCG, 31QDD, 31QDE, 31QDF, 31QDG"),
            AREA_71(184, 110, 189, 115, 71, "area_71", "31QGD, 31QGE, 31QGF, 31QGG, 31QHD, 31QHE, 31QHF, 31QHG, 32QKJ, 32QKK, 32QKL, 32QKM, 32QLJ, 32QLK, 32QLL, 32QLM"),
            AREA_72(189, 110, 194, 115, 72, "area_72", "32QPJ, 32QPK, 32QPL, 32QPM, 32QQJ, 32QQK, 32QQL, 32QQM, 32QRJ, 32QRK, 32QRL, 32QRM, 33QTD, 33QTE, 33QTF, 33QTG"),
            AREA_73(194, 110, 199, 115, 73, "area_73", "33QVD, 33QVE, 33QVF, 33QVG, 33QWD, 33QWE, 33QWF, 33QWG, 33QXD, 33QXE, 33QXF, 33QXG, 33QYD, 33QYE, 33QYF, 33QYG, 33QZD, 33QZE"),
            AREA_74(199, 110, 204, 115, 74, "area_74", "34QCJ, 34QCK, 34QCL, 34QCM, 34QDJ, 34QDK, 34QDL, 34QDM, 34QEJ, 34QEK, 34QEL, 34QEM, 34QFJ, 34QFK, 34QFL, 34QFM, 34QGJ"),
            AREA_75(204, 110, 209, 115, 75, "area_75", "35QKD, 35QKE, 35QKF, 35QKG, 35QLD, 35QLE, 35QLF, 35QLG, 35QMD, 35QME, 35QMF, 35QMG, 35QND, 35QNE, 35QNF, 35QNG"),
            AREA_76(209, 110, 214, 115, 76, "area_76", "35QRD, 35QRE, 35QRF, 35QRG, 36QTJ, 36QTK, 36QTL, 36QTM, 36QUJ, 36QUK, 36QUL, 36QUM, 36QVJ, 36QVK, 36QVL, 36QVM"),
            AREA_77(214, 110, 219, 115, 77, "area_77", "36QYJ, 36QYK, 36QYL, 36QYM, 36QZJ, 36QZK, 36QZL, 36QZM, 37QBD, 37QBE, 37QBF, 37QBG, 37QCD, 37QCE, 37QCF, 37QCG"),
            AREA_78(219, 110, 224, 115, 78, "area_78", "37QFD, 37QFE, 37QFF, 37QFG, 37QGD, 37QGE, 37QGF, 37QGG, 37QHD, 37QHE, 37QHF, 37QHG, 38QKJ, 38QKK, 38QKL, 38QKM"),
            AREA_79(224, 110, 229, 115, 79, "area_79", "38QMJ, 38QMK, 38QML, 38QMM, 38QNJ, 38QNK, 38QNL, 38QNM, 38QPJ, 38QPK, 38QPL, 38QPM, 38QQJ, 38QQK, 38QQL, 38QQM, 38QRJ, 38QRK"),
            AREA_80(229, 110, 233, 115, 80, "area_80", "39QUD, 39QUE, 39QUF, 39QUG, 39QVD, 39QVE, 39QVF, 39QVG, 39QWD, 39QWE, 39QWF, 39QWG");

            final int left;
            final int top;
            final int right;
            final int bottom;
            final int index;
            final String nicename;
            final String tiles;

            S2PixelProductArea(int left, int top, int right, int bottom, int index, String nicename, String tiles) {
                this.left = left;
                this.top = top;
                this.right = right;
                this.bottom = bottom;
                this.index = index;
                this.nicename = nicename;
                this.tiles = tiles;
            }
        }

        @Override
        public PixelProductArea getArea(String identifier) {
            return translate(S2PixelProductArea.valueOf(identifier));
        }

        @Override
        public PixelProductArea[] getAllAreas() {
            PixelProductArea[] result = new PixelProductArea[S2PixelProductArea.values().length];
            S2PixelProductArea[] values = S2PixelProductArea.values();
            for (int i = 0; i < values.length; i++) {
                S2PixelProductArea area = values[i];
                result[i] = translate(area);
            }
            return result;
        }

        private static PixelProductArea translate(S2PixelProductArea mppa) {
            return new PixelProductArea(mppa.left, mppa.top, mppa.right, mppa.bottom, mppa.index, mppa.nicename, mppa.tiles);
        }
    }
}

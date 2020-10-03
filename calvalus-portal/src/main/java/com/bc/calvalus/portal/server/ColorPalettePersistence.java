package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.DtoColorPalette;

import java.io.*;
import java.util.*;

/**
 * A location that stores color palettes.
 *
 * @author Declan
 */
public class ColorPalettePersistence {

    private final String userName;
    private final File userAppDataDir;

    public ColorPalettePersistence(String userName, File userAppDataDir) {
        this.userName = userName;
        this.userAppDataDir = userAppDataDir;
    }

    public DtoColorPalette[] loadColorPalettes() throws IOException {
        Properties colorPaletteProperties = loadDefaultColorPalettes();
        Properties userColorPaletteProperties = loadUserColorPalettes();
        colorPaletteProperties.putAll(userColorPaletteProperties);

        ArrayList<DtoColorPalette> colorPalettes = new ArrayList<DtoColorPalette>();
        Set<String> colorPaletteNames = colorPaletteProperties.stringPropertyNames();
        for (String colorPaletteFullName : colorPaletteNames) {
            String[] split = colorPaletteFullName.split("\\.");
            String colorPaletteName = split[split.length - 1];
            String[] colorPalettePath = Arrays.copyOf(split, split.length - 1);
            String cpdURL = colorPaletteProperties.getProperty(colorPaletteFullName);
            DtoColorPalette colorPalette = new DtoColorPalette(colorPaletteName, colorPalettePath, cpdURL);
            colorPalettes.add(colorPalette);
        }

        Collections.sort(colorPalettes, new Comparator<DtoColorPalette>() {
            @Override
            public int compare(DtoColorPalette o1, DtoColorPalette o2) {
                return o1.getQualifiedName().compareToIgnoreCase(o2.getQualifiedName());
            }
        });
        return colorPalettes.toArray(new DtoColorPalette[colorPalettes.size()]);
    }

    public void storeColorPalettes(DtoColorPalette[] colorPalettes) throws IOException {
        Properties userColorPalettes = getUserColorPalettes(colorPalettes);
        File file = getColorPaletteFile(getUserName());
        file.getParentFile().mkdirs();
        FileWriter fileWriter = new FileWriter(file);
        try {
            userColorPalettes.store(fileWriter, "Calvalus color palettes for user " + getUserName());
        } finally {
            fileWriter.close();
        }
    }

    private File getColorPaletteFile(String user) {
        return new File(userAppDataDir, user + "-color-palettes.properties");
    }

    private String getUserName() {
        return userName;
    }

    private Properties getUserColorPalettes(DtoColorPalette[] colorPalettes) {
        Properties userColorPalettes = new Properties();
        for (DtoColorPalette colorPalette : colorPalettes) {
            if (colorPalette.isUserColorPalette()) {
                String fullName = colorPalette.getQualifiedName();
                System.out.println("storing color palette " + fullName + " = " + colorPalette.getCpdURL());
                userColorPalettes.put(fullName, colorPalette.getCpdURL());
            }
        }
        return userColorPalettes;
    }

    private Properties loadDefaultColorPalettes() throws IOException {
        InputStream stream = getClass().getResourceAsStream("color-palettes.properties");
        Properties systemColorPalettes = loadColorPalettes(new BufferedReader(new InputStreamReader(stream)));

        File additionalSystemColorPaletteFile = getColorPaletteFile("SYSTEM");
        if (additionalSystemColorPaletteFile.exists()) {
            Properties additionalSystemColorPalettes = loadColorPalettes(new FileReader(additionalSystemColorPaletteFile));
            systemColorPalettes.putAll(additionalSystemColorPalettes);
        }
        return systemColorPalettes;
    }

    private Properties loadUserColorPalettes() throws IOException {
        File userColorPaletteFile = getColorPaletteFile(getUserName());
        if (userColorPaletteFile.exists()) {
            return loadColorPalettes(new FileReader(userColorPaletteFile));
        } else {
            return new Properties();
        }
    }

    private Properties loadColorPalettes(Reader stream) throws IOException {
        Properties colorPalettes = new Properties();
        try {
            colorPalettes.load(stream);
        } finally {
            stream.close();
        }
        return colorPalettes;
    }

}

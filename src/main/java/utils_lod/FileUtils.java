package utils_lod;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

public class FileUtils {

    private final static Logger LOGGER = Logger.getLogger(FileUtils.class.getName());

    /**
     * Gets the set of file names, from any directory structure recursively.
     *
     * @param path
     * @param fileList
     */
    public static void getFilesList(String path, Set<String> fileList) {
        File dir = new File(path);

        if (dir.isFile()) {
            fileList.add(path);
        } else if (dir.isDirectory()) {
            String[] list = dir.list();
            for (String item : list) {
                getFilesList(path + "/" + item, fileList);
            }
        }
    }

    /**
     * Saves textual data content, into a file.
     *
     * @param text
     * @param path
     */
    public static void saveText(String text, String path) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(path));
            writer.append(text);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
    }

    /**
     * Saves textual data content, into a file.
     *
     * @param text
     * @param path
     * @param append
     */
    public static void saveText(String text, String path, boolean append) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(path, append));
            writer.append(text);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
    }

    /**
     * Writes the content of an object, into a file.
     *
     * @param obj
     * @param path
     */
    public static void saveObject(Object obj, String path) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
            out.writeObject(obj);
            out.flush();
            out.close();
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
    }

    /**
     * Check if a file exists
     *
     * @param path
     * @param isDebug
     * @return
     */
    public static boolean fileExists(String path, boolean isDebug) {
        boolean rst = new File(path).exists();

        if (!rst && isDebug) {
            LOGGER.info("File doesnt exist... [" + path + "]");
        }

        return rst;
    }

    /**
     * Reads the object content from a file, and later from the called method is casted to the correct type.
     *
     * @param path
     * @return
     */
    public static Object readObject(String path) {
        if (fileExists(path, false)) {
            try {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(path));
                Object obj = in.readObject();

                in.close();
                return obj;
            } catch (Exception e) {
                LOGGER.severe(e.getCause().toString());
            }
        }
        return null;
    }

    /**
     * Reads the textual contents from a file.
     *
     * @param path
     * @return
     */
    public static String readText(String path) {
        try {
            StringBuffer sb = new StringBuffer();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            while (reader.ready()) {
                String line = reader.readLine();
                if (line.startsWith("#")) {
                    continue;
                }

                sb.append(line);
                sb.append("\n");
            }
            reader.close();
            return sb.toString();
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
        return "";
    }

    /**
     * Reads the textual contents from a file into  a set split based on a specific delimiter.
     *
     * @param path
     * @param delimiter
     * @param changeCase
     * @return
     */
    public static Set<String> readIntoSet(String path, String delimiter, boolean changeCase) {
        if (!FileUtils.fileExists(path, true)) {
            return null;
        }

        Set<String> rst = new HashSet<String>();

        String content = readText(path);
        String[] tmp = content.split(delimiter);
        for (String s : tmp) {
            if (changeCase) {
                rst.add(s.trim().toLowerCase());
            } else {
                rst.add(s);
            }
        }

        return rst;
    }

    /**
     * Reads the textual contents from a file into  a Map<String, String> split based on a specific delimiter.
     *
     * @param path
     * @param delimiter
     * @param changeCase
     * @return
     */
    public static Map<String, String> readIntoStringMap(String path, String delimiter, boolean changeCase) {
        Set<String> lines = readIntoSet(path, "\n", changeCase);
        Map<String, String> rst = new TreeMap<String, String>();

        for (String line : lines) {
            String[] tmp = line.split(delimiter);
            if (tmp.length == 2) {
                rst.put(tmp[0].trim(), tmp[1]);
            }
        }

        return rst;
    }
}

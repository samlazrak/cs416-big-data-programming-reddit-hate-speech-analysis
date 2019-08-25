package bdp.reddit_hate;

import com.uttesh.exude.ExudeData;
import com.uttesh.exude.stemming.Stemmer;
import com.uttesh.exude.exception.InvalidDataException;

import java.util.StringTokenizer;
import java.util.Arrays;

public class TokenizerStemmer {
    
    static String hateWordsString = "Africoon Afro-Saxon Americoon Amo Angie Anglo Ann Argie Armo Bengali Buckwheat Buddhahead Caublasian Charlie Chinaman Cushi Cushite FOB Fairy Gerudo Gwat Honyak Honyock Hunkie Hunky Hunyak Hunyock Jap Jerry Jewbacca Jihadi Kushi Kushite Leb Lebbo Merkin Moor Orangie Oreo Oriental Pepper Pepsi Punjab Russellite  SAWCSM Shelta Shy Shylock Skip Skippy Taffy Tommy Twinkie WASP WIC Whipped Yank Yankee ZOG Zionazi abbo abo af albino ape azn banana beaner beaney bhrempti bint bitch blaxican blockhead bludger bluegum bogan boo boojie boon booner boxhead bubble buck buckethead buckra buffie bumblebee burrhead butterhead celestial charva charver chav chigger chinig chink chonky chug chunky clam clamhead colored coloured coolie coon cracker cripple crow cunt dago darkey darkie darky dego dhimmi dinge dink domes dyke eurotrash eyetie fag faggot fez fuzzy gable gew ghetto gin ginzo gipp gippo golliwog goober gook gooky greaseball greaser groid guala gub gubba guido guinea gurrier gyp gypo gyppie gyppo gyppy hairyback halfrican hayseed hebe hebro heeb heinie hick higger hillbilly ho hoe honkey honkie honky hoosier hoser hymie idiot ike ikey iky injun jant japie jhant jig jigaboo jigarooni jigg jigga jiggabo jiggaboo jigger jijjiboo jock jockie jocky khazar kike knacker kotiya kraut kyke latrino lemonhead leprechaun limey lowlander lubra lugan mack mansplaining mick mickey millie moch mock mockey mockie mocky moke moky mong monkey mook mosshead moulie moulignon moulinyan moxy muk muktuk mulato mulignan mung munt munter mutt muzzie nacho neche ned neechee neejee negro newfie nicca nichi nichiwa nidge nig nigar niger nigette nigga niggah niggar nigger nigglet niggor niggress nigguh niggur niglet nigor nigra nigre nip nitchee nitchie nitchy ocker octaroon octroon ofay paddy paki paleface pancake papist papoose patriarchy peckerwood pickaninny pig piker pikey piky pinto pogue polack pollo popolo poppadom powderburn prod proddywhoddy proddywoddy property pussy quadroon quashie queen queer raghead redlegs redneck redskin retard retarded roofucker roundeye rube sambo sawney scag scallie scally scanger semihole senga seppo septic shade shant sheeny sheepfucker sheister shine shiner shyster skag skanger skin skinhead skinny slag slant slit slope slopehead slopey slopy snout snowflake sole sooty spade sperg spic spick spickaboo spide spig spigger spigotty spik spike spink spiv spook squarehead squaw squinty steek stovepipe suntan tan teabagger teapot tenker thicklips tiger tincker tinkar tinkard tinker tinkere tranny trash twat tyncar tynekere tynkard tynkare tynker tynkere uncivilised uncivilized wetback wexican whigger whitey wigga wigger wiggerette wink wog wop yardie yellow yid yob yobbo yokel zebra zigabo zip zipperhead zippohead";

    static ExudeData exudeData = null;
    static Stemmer stemmer = null;
    static String[] stemmedHateWords = null;
    static String[] hateWords = null;
    
    public static String getHateWords(String input) {
        return getHateWords(input, false);
    }
    
    public static String getHateWords(String input, Boolean debug) {
        if (debug == true) {
            String output = filter(input);
            System.out.println("filtered   : " + output);
            output = stem(output);
            System.out.println("stemmed    : " + output);
            output = findHateMatches(output);
            System.out.println("hate words : " + output);
            return output;
        } else
            return findHateMatches(stem(filter(input)));
    }
    
    public static String filter(String input) {
        if (exudeData == null)
            exudeData = ExudeData.getInstance();
        
        try {
            return exudeData.filterStoppingsKeepDuplicates(input);
        } catch (InvalidDataException e) {
            e.printStackTrace();
        }
        return "";
    }
    
    public static String stem(String input) {
        if (stemmer == null)
            stemmer = new Stemmer();
        
        String output = "";
        StringTokenizer st = new StringTokenizer(input);
        while (st.hasMoreTokens()) {
            output += stemmer.stem(st.nextToken()) + " ";
        }
        return output;
    }
    
    public static String findHateMatches(String input) {
        if (stemmedHateWords == null) {
            hateWords = hateWordsString.split("\\s+");
            stemmedHateWords = stem(hateWordsString).split("\\s+");
        }
        
        String output = "";
        StringTokenizer st = new StringTokenizer(input);
        
        int matchIndex;
        
        while (st.hasMoreTokens()) {
            matchIndex = Arrays.binarySearch(stemmedHateWords, st.nextToken());
            if (matchIndex >= 0)
                output += hateWords[matchIndex] + " ";
        }
        return output;
    }
}
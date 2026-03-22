# 🛒 מחפש מחירי סופרמרקטים

מערכת חיפוש ומעקב אחרי מחירי מוצרים מהרשתות הגדולות בישראל.  
פועל **מקומית במחשב** — אין צורך ב-IIS, שרת חיצוני, או התקנת תוכנות מסובכות.

---

## ⚡ הפעלה מהירה

**לחץ פעמיים על `start.bat`**

הקובץ יבצע הכל אוטומטית:
- בודק אם Node.js מותקן
- מתקין חבילות אם צריך (פעם ראשונה בלבד)
- מפעיל את השרת
- פותח את הדפדפן אוטומטית

---

## 📋 דרישות מוקדמות

### Node.js (חובה)

Node.js הוא סביבת ריצה ל-JavaScript — כמו Java Runtime, רק ל-Node.

**בדיקה אם מותקן:**
```
פתח שורת פקודה (cmd) והקלד: node --version
```
אם מקבל מספר גרסה (כגון `v22.0.0`) — **אין צורך בהתקנה**.  
אם מקבל שגיאה — יש להתקין:

**התקנה:**
1. גש ל: **https://nodejs.org**
2. לחץ על הכפתור הירוק הגדול **"Download Node.js (LTS)"**
3. הרץ את ה-installer שהורד
4. לחץ Next → Next → Install (ברירות מחדל מתאימות)
5. **הפעל מחדש את המחשב** לאחר ההתקנה
6. לחץ פעמיים על `start.bat`

---

## 🖥️ שימוש

| פעולה | איך |
|---|---|
| **הפעלה** | לחץ פעמיים על `start.bat` |
| **עצירה** | לחץ `Ctrl+C` בחלון הפקודות |
| **שינוי פורט** | ערוך `start.bat` והוסף `set PORT=8080` לפני שורת node |
| **גישה ממחשב אחר** | `http://[IP-של-המחשב]:3000` |

### טעינת נתונים ראשונית

1. פתח את הדפדפן ב-`http://localhost:3000`
2. לחץ **"טען נתונים"**
3. המתן 2-5 דקות לסיום השליפה
4. הנתונים נשמרים ב-`prices-data.json` ונטענים אוטומטית בהפעלה הבאה

---

## 🏪 רשתות נתמכות

| רשת | פלטפורמה | URL |
|---|---|---|
| רמי לוי | Cerberus | publishedprices.co.il |
| יוחננוף | Cerberus | publishedprices.co.il |
| אושר עד | Cerberus | publishedprices.co.il |
| חצי חינם | Cerberus | publishedprices.co.il |
| קשת טעמים | Cerberus | publishedprices.co.il |
| דורלון | Cerberus | publishedprices.co.il |
| סופר דוש | Cerberus | publishedprices.co.il |
| ויקטורי | Nibit/Matrix | matrixcatalog.co.il |
| מחסני השוק | Nibit/Matrix | matrixcatalog.co.il |
| מחסני להב | Nibit/Matrix | matrixcatalog.co.il |
| שופרסל | עצמאי | prices.shufersal.co.il |
| קרפור | עצמאי | storefiles.carrefour.co.il |
| טיב טעם | עצמאי | tivtaam.co.il |
| קואופ | עצמאי | coopisrael.coop |
| עדן טבע | עצמאי | edenteva.co.il |

> **הערה:** חלק מהאתרים חסומים מחוץ לישראל. יש להריץ מרשת ישראלית.

---

## 📂 הוספת קובץ XML ידנית

אם רשת מסוימת לא נטענת אוטומטית, אפשר להוסיף קובץ ישיר:

1. גש לאתר הרשת ומצא קובץ `PriceFull*.xml.gz`
2. העתק את ה-URL
3. בממשק: **"הוסף קובץ XML ישיר"** → בחר רשת → הדבק URL → לחץ הוסף

### כתובות ידועות לקבצים:

**שופרסל** (רשימת קבצים):
```
http://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2&storeId=0&page=1
```

**ויקטורי / מחסני השוק** (רשימת קבצים):
```
http://matrixcatalog.co.il/NBCompetitionData.aspx
```

**קרפור**:
```
https://storefiles.carrefour.co.il/
```

**רמי לוי / יוחננוף / שאר Cerberus**:
```
https://url.retail.publishedprices.co.il/
```

---

## 🔧 פרטים טכניים

| רכיב | פרטים |
|---|---|
| שרת | Node.js + Express |
| אחסון | קובץ JSON מקומי (`prices-data.json`) |
| פורט ברירת מחדל | 3000 |
| גישה | דפדפן רגיל, ללא התחברות |
| עדכון אוטומטי | ידני (כפתור "טען נתונים") |

---

## ❓ שאלות נפוצות

**השרת לא מופעל / שגיאה:**
- וודא ש-Node.js מותקן: `node --version` בשורת פקודה
- נסה להריץ `start.bat` כמנהל (קליק ימני → "הפעל כמנהל")

**לא נטענו מוצרים:**
- בדוק חיבור לאינטרנט
- חלק מהאתרים חסומים מחוץ לישראל
- נסה להוסיף URL ישיר מהסעיף למעלה

**השרת רץ אבל הדפדפן לא נפתח:**
- פתח ידנית: http://localhost:3000

**שינוי פורט:**
```bat
set PORT=8080
node server.js
```

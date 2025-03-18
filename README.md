If you enciunter the following error:
```
java.lang.reflect.InaccessibleObjectException: Unable to make field private final java.lang.Object[] java.util.Arrays$ArrayList.a accessible: module java.base does not "opens java.util" to unnamed module @1ad282e0
```
Then you need to add the following VM option to your run configuration:
```bash
--add-opens java.base/java.util=ALL-UNNAMED
```
You can do this in IntelliJ by going to the following menu:
Run—>EditConfigurations—>Modify options—>JAVA Add VM options—>VM options
 

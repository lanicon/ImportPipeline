      <datasource name="$$name$$" type="TikaDS" trim="true" maxparallel="0" tikaurl='http://localhost:8080/convert?compress=true&amp;file=/' tikaprocess='tika' pingurl='http://localhost:8080/ping' pingtimeout='30000' >
         <security emit='true' wellknown="everyone" />
         <dbgstore></dbgstore>
         <provider type="FileNameFeeder" root="data_in" filter="\.(msg|eml)$" virtualroot="ma" filesort='filename' ignoredates='true' />
         <provider type="FileNameFeeder" root="data_in" filter="\.(pdf)$" virtualroot="doc" filesort='filename' ignoredates='true' />
      </datasource>

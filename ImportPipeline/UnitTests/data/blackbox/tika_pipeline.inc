      <pipeline name="$$name$$" converters="trim" script="ScriptExtensions" postprocessors="$$post$$">
         <action key="record" type='cat' categories="all"/>
         <action key="record" type='del' skipuntil='*' cond='doc_cat,eq|substring,delete'/>
         <action key="record" type='add' script="OnAdd" />
         <action key="record/_start" field="filename"/>
         <_action key="record/_start" field="errorFileName" endpoint="es.errors_" />
         <action key="record/_istextmail" field="isTextMail" />
         <action key="record/_numparts" field="numparts" />
         <action key="record/_numattachments" field="numattachments" />
         <action key="record/_filesize" field="filesize" />
         <action key="record/_checkexist" type="checkexist" keysource="value:FullName" datesource="value:LastModified" debug="true"/>
         <action key="record/lastmodutc" field="filedate" converters="date"/>
         <action key="record/virtualFilename" field="virtualFilename" />
         <action key="record/virtualRoot" field="virtualRoot" />
         <action key="record/converted_file" field="convertedFilename" />
         <action key="record/page_count|record/xmptpg:npages" field="page_count" converters="int32"/>
         <action key="record/subject" field="subject" converters="htmlDecode" />
         <action key="record/keywords" field="keywords" converters="htmlDecode"/>
         <action key="record/head" field="head" />
         <action key="record/content" field="content" />
         <action key="record/shortcontent" field="shortcontent" />
         <action key="record/Message-From" field="author" flags="unique|toarray"  converters="htmlDecode"/>
         <action key="record/Message-To" field="recip" flags="unique|toarray"  converters="htmlDecode"/>
         <action key="record/Message-CC" field="recip_cc" flags="unique|toarray"  converters="htmlDecode"/>
         <action key="record/Message-ID" field="msg_id" />
         <action key="record/Message-ID" field="msg_ref" flags="unique|toarray" />
         <action key="record/In-Reply-To" field="msg_ref" flags="unique|toarray"/>
         <action key="record/References" field="msg_ref" flags="unique|toarray"/>
         <action key="record/security/group/allow" field="secgroups" flags="unique|toarray" converters="string"/>

         <template expr="^record/.*(save-date|modified|save-date)$" field="date_modified" convert="date" flags="keepSmallest"/>
         <template expr="^record/.*(creation-date|created)$" field="date_created" converters="date" flags="keepSmallest"/>
         <template expr="^record/.*title$" field="title" converters="htmlDecode"/>
         <template expr="^record/.*(author|creator)$" field="author" flags="unique|toarray" converters="htmlDecode"/>
         
         <_action key="record/_error" type="errorhandler" endpoint="es.errors_" />

         <template expr="^record/content.type$" field="doctype" converters="doctype"/>
         <template expr="^record/content.type$" field="content_type"/>
         <template expr="(^record/(_|X-Parsed-By|Content-Length|resourceName))" nop='true' />
         <template expr="^record/(.*)" field="__$1" />
      </pipeline>
